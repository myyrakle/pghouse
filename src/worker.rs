use crate::catalog::{
    TableConfig, apply_mutation, dirty_row_count, load_candidate_tables, load_column_specs,
    load_live_rows, load_pending_rows, load_table_config, mark_merge_failure, next_generation,
    pending_merge_count,
};
use crate::compression::JsonArrayZstdCodec;
use crate::interface::{
    ChunkCodec, ColumnChunkEncodeRequest, ColumnDescriptor, GranuleSpan, GranuleWriteBatch,
    GranuleWriteRequest, RowVersion, SnapshotWriter, TableDescriptor,
};
use crate::storage::FileSnapshotWriter;
use anyhow::{Context, Result};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::{JsonB, pg_guard, pg_sys, warning};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Default, Serialize)]
pub(crate) struct MaintenanceStats {
    pub(crate) tables_scanned: usize,
    pub(crate) pending_rows_applied: usize,
    pub(crate) merges_completed: usize,
    pub(crate) live_rows_rewritten: usize,
}

pub(crate) fn run_maintenance_cycle(
    max_tables: i32,
    pending_limit: i32,
) -> Result<MaintenanceStats> {
    let mut stats = MaintenanceStats::default();

    for table_oid in load_candidate_tables(max_tables)? {
        stats.tables_scanned += 1;

        match maintain_table(table_oid, pending_limit) {
            Ok(table_stats) => {
                stats.pending_rows_applied += table_stats.pending_rows_applied;
                stats.merges_completed += table_stats.merges_completed;
                stats.live_rows_rewritten += table_stats.live_rows_rewritten;
            }
            Err(error) => {
                warning!(
                    "pghouse maintenance failed for table oid {}: {error:#}",
                    table_oid
                );
                let _ = mark_merge_failure(table_oid, &format!("{error:#}"));
            }
        }
    }

    Ok(stats)
}

fn maintain_table(table_oid: i64, pending_limit: i32) -> Result<MaintenanceStats> {
    let config = load_table_config(table_oid)?;
    let pending_rows = load_pending_rows(table_oid, pending_limit)?;

    let mut stats = MaintenanceStats::default();
    for mutation in &pending_rows {
        apply_mutation(mutation, table_oid)?;
    }
    stats.pending_rows_applied = pending_rows.len();

    let dirty_rows = dirty_row_count(table_oid)?;
    let queued_merges = pending_merge_count(table_oid)?;
    if dirty_rows == 0 && queued_merges == 0 {
        return Ok(stats);
    }

    if dirty_rows >= i64::from(config.granule_rows) || queued_merges > 0 {
        stats.live_rows_rewritten = rewrite_table_snapshot(&config)?;
        stats.merges_completed = 1;
    }

    Ok(stats)
}

fn rewrite_table_snapshot(config: &TableConfig) -> Result<usize> {
    let live_rows = load_live_rows(config.table_oid)?;
    let columns = load_column_specs(config.table_oid)?;
    let new_base_generation = next_generation(config.table_oid)?;
    let table = TableDescriptor::from(config);
    let codec = JsonArrayZstdCodec;
    let writer = FileSnapshotWriter;

    let mut parsed_rows = Vec::with_capacity(live_rows.len());
    for live_row in &live_rows {
        let row = serde_json::from_str::<Value>(&live_row.row_json)
            .with_context(|| format!("failed to parse row JSON for pk {}", live_row.pk_text))?;
        parsed_rows.push(RowVersion {
            pk_text: live_row.pk_text.clone(),
            row_json: row,
        });
    }

    if parsed_rows.is_empty() {
        writer.replace_snapshot(&GranuleWriteRequest {
            table,
            granules: Vec::new(),
        })?;
        return Ok(0);
    }

    let granule_size = usize::try_from(config.granule_rows.max(1)).unwrap_or(1);
    let column_descriptors = columns
        .iter()
        .map(|column| ColumnDescriptor {
            ordinal: column.attnum,
            name: column.attname.clone(),
        })
        .collect::<Vec<_>>();
    let mut granules = Vec::new();

    for (index, chunk) in parsed_rows.chunks(granule_size).enumerate() {
        let generation = new_base_generation + i64::try_from(index).unwrap_or_default();
        let pk_min = chunk.first().map(|row| row.pk_text.clone());
        let pk_max = chunk.last().map(|row| row.pk_text.clone());

        let mut encoded_chunks = Vec::with_capacity(column_descriptors.len());
        for column in &column_descriptors {
            let request = ColumnChunkEncodeRequest {
                table: table.clone(),
                column: column.clone(),
                rows: chunk.to_vec(),
                codec: table.compression.clone(),
            };
            encoded_chunks.push(codec.encode(&request)?);
        }

        granules.push(GranuleWriteBatch {
            span: GranuleSpan {
                generation,
                row_count: i32::try_from(chunk.len()).unwrap_or(i32::MAX),
                pk_min,
                pk_max,
                merge_reason: "background_merge".to_string(),
            },
            chunks: encoded_chunks,
        });
    }

    writer.replace_snapshot(&GranuleWriteRequest { table, granules })?;

    Ok(parsed_rows.len())
}

#[pg_extern]
fn pghouse_run_maintenance_once(
    max_tables: default!(i32, 8),
    pending_limit: default!(i32, 65_536),
) -> JsonB {
    let stats = run_maintenance_cycle(max_tables, pending_limit)
        .unwrap_or_else(|error| error!("pghouse maintenance failed: {error:#}"));
    JsonB(serde_json::to_value(stats).unwrap())
}

#[pg_extern]
fn pghouse_launch_worker(
    max_tables: default!(i32, 8),
    pending_limit: default!(i32, 65_536),
    sleep_ms: default!(i32, 5_000),
    iterations: default!(i32, 0),
) -> bool {
    let database_oid = unsafe { pg_sys::MyDatabaseId.to_u32() };
    let user_oid = unsafe { pg_sys::GetUserId().to_u32() };
    let extra =
        format!("{database_oid}:{user_oid}:{max_tables}:{pending_limit}:{sleep_ms}:{iterations}");

    BackgroundWorkerBuilder::new("pghouse merge worker")
        .set_library("pghouse")
        .set_function("pghouse_dynamic_worker_main")
        .enable_spi_access()
        .set_extra(&extra)
        .load_dynamic()
        .is_ok()
}

#[pg_guard]
pub extern "C-unwind" fn pghouse_dynamic_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);

    let extra = BackgroundWorker::get_extra().to_string();
    let mut parts = extra.split(':');
    let database_oid = parts.next().and_then(|value| value.parse::<u32>().ok());
    let user_oid = parts.next().and_then(|value| value.parse::<u32>().ok());
    let max_tables = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(8);
    let pending_limit = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(65_536);
    let sleep_ms = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    let iterations = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);

    BackgroundWorker::connect_worker_to_spi_by_oid(
        database_oid.map(pg_sys::Oid::from),
        user_oid.map(pg_sys::Oid::from),
    );

    let mut remaining_iterations = iterations;
    loop {
        if let Err(error) = run_maintenance_cycle(max_tables, pending_limit) {
            warning!("pghouse worker cycle failed: {error:#}");
        }

        if remaining_iterations > 0 {
            remaining_iterations -= 1;
            if remaining_iterations == 0 {
                break;
            }
        }

        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(sleep_ms))) {
            break;
        }
    }
}
