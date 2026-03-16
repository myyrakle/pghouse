use crate::core::codec::JsonArrayZstdCodec;
use crate::core::file_layout::read_json_lines;
use crate::core::interface::{ColumnDescriptor, RowVersion, SnapshotWriter};
use crate::core::pk::compare_pk_text;
use crate::core::snapshot::build_snapshot_write_request;
use crate::pg::catalog::{
    InsertBuffer, TableConfig, apply_mutation, claim_ready_insert_buffers, dirty_row_count,
    load_candidate_tables, load_column_specs, load_live_rows, load_pending_rows, load_table_config,
    mark_flush_success, mark_insert_buffer_flushed, mark_merge_failure, next_generation,
    pending_merge_count, release_insert_buffer_claim, release_table_maintenance_lock,
    try_acquire_table_maintenance_lock, upsert_buffered_row_version,
};
use crate::pg::storage::{FileSnapshotWriter, append_granules, insert_buffer_absolute_path};
use anyhow::{Context, Result};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::{JsonB, pg_guard, pg_sys, warning};
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::time::Duration;

#[derive(Debug, Default, Serialize)]
pub(crate) struct MaintenanceStats {
    pub(crate) tables_scanned: usize,
    pub(crate) buffered_rows_flushed: usize,
    pub(crate) granules_appended: usize,
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
                stats.buffered_rows_flushed += table_stats.buffered_rows_flushed;
                stats.granules_appended += table_stats.granules_appended;
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
    if !try_acquire_table_maintenance_lock(table_oid)? {
        return Ok(MaintenanceStats::default());
    }

    let result = maintain_table_locked(table_oid, pending_limit);
    let unlock_result = release_table_maintenance_lock(table_oid);

    match (result, unlock_result) {
        (Ok(stats), Ok(())) => Ok(stats),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(_)) => Err(error),
    }
}

fn maintain_table_locked(table_oid: i64, pending_limit: i32) -> Result<MaintenanceStats> {
    let config = load_table_config(table_oid)?;

    let mut stats = MaintenanceStats::default();
    let flushed = flush_insert_buffers(&config)?;
    stats.buffered_rows_flushed = flushed.rows_flushed;
    stats.granules_appended = flushed.granules_written;

    let pending_rows = load_pending_rows(table_oid, pending_limit)?;
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

#[derive(Debug, Default)]
struct FlushStats {
    rows_flushed: usize,
    granules_written: usize,
}

#[derive(Debug, serde::Deserialize)]
struct BufferedInsertRecord {
    pk_text: String,
    row_json: Value,
}

fn flush_insert_buffers(config: &TableConfig) -> Result<FlushStats> {
    let claimed_buffers = claim_ready_insert_buffers(config.table_oid, 64)?;
    if claimed_buffers.is_empty() {
        return Ok(FlushStats::default());
    }

    let columns = load_column_specs(config.table_oid)?;
    let column_descriptors = columns
        .iter()
        .map(|column| ColumnDescriptor {
            ordinal: column.attnum,
            name: column.attname.clone(),
        })
        .collect::<Vec<_>>();
    let table = crate::core::interface::TableDescriptor::from(config);
    let codec = JsonArrayZstdCodec;
    let mut next_generation_value = next_generation(config.table_oid)?;
    let mut stats = FlushStats::default();

    for (index, buffer) in claimed_buffers.iter().enumerate() {
        match flush_single_insert_buffer(
            config,
            &table,
            &column_descriptors,
            &codec,
            buffer,
            &mut next_generation_value,
        ) {
            Ok(result) => {
                mark_flush_success(config.table_oid)?;
                stats.rows_flushed += result.rows_flushed;
                stats.granules_written += result.granules_written;
            }
            Err(error) => {
                let _ = release_insert_buffer_claim(buffer.id);
                for remaining in claimed_buffers.iter().skip(index + 1) {
                    let _ = release_insert_buffer_claim(remaining.id);
                }
                return Err(error);
            }
        }
    }

    Ok(stats)
}

#[derive(Debug, Default)]
struct SingleBufferFlushResult {
    rows_flushed: usize,
    granules_written: usize,
}

fn flush_single_insert_buffer(
    config: &TableConfig,
    table: &crate::core::interface::TableDescriptor,
    columns: &[ColumnDescriptor],
    codec: &JsonArrayZstdCodec,
    buffer: &InsertBuffer,
    next_generation_value: &mut i64,
) -> Result<SingleBufferFlushResult> {
    let absolute_path = insert_buffer_absolute_path(&config.storage_root, &buffer.storage_path);
    let mut rows = read_json_lines::<BufferedInsertRecord>(&absolute_path)?;
    if rows.is_empty() {
        mark_insert_buffer_flushed(buffer.id)?;
        fs::remove_file(&absolute_path).with_context(|| {
            format!(
                "failed to remove empty flushed insert buffer file {}",
                absolute_path.display()
            )
        })?;
        return Ok(SingleBufferFlushResult::default());
    }

    rows.sort_by(|left, right| compare_pk_text(table.pk_compare, &left.pk_text, &right.pk_text));

    let mut row_versions = Vec::with_capacity(rows.len());
    for row in &rows {
        upsert_buffered_row_version(
            config.table_oid,
            &row.pk_text,
            &serde_json::to_string(&row.row_json)
                .with_context(|| format!("failed to serialize buffered row {}", row.pk_text))?,
        )?;
        row_versions.push(RowVersion {
            pk_text: row.pk_text.clone(),
            row_json: row.row_json.clone(),
        });
    }

    let request = build_snapshot_write_request(
        table.clone(),
        columns,
        &row_versions,
        *next_generation_value,
        "buffer_flush",
        codec,
    )?;
    let result = append_granules(&request)?;
    *next_generation_value += i64::try_from(result.granules_written).unwrap_or_default();
    mark_insert_buffer_flushed(buffer.id)?;
    fs::remove_file(&absolute_path).with_context(|| {
        format!(
            "failed to remove flushed insert buffer file {}",
            absolute_path.display()
        )
    })?;

    Ok(SingleBufferFlushResult {
        rows_flushed: row_versions.len(),
        granules_written: result.granules_written,
    })
}

fn rewrite_table_snapshot(config: &TableConfig) -> Result<usize> {
    let live_rows = load_live_rows(config.table_oid, config.pk_compare)?;
    let columns = load_column_specs(config.table_oid)?;
    let base_generation = next_generation(config.table_oid)?;
    let table = crate::core::interface::TableDescriptor::from(config);
    let codec = JsonArrayZstdCodec;
    let writer = FileSnapshotWriter;

    let parsed_rows = parse_live_rows(&live_rows)?;
    let column_descriptors = columns
        .iter()
        .map(|column| ColumnDescriptor {
            ordinal: column.attnum,
            name: column.attname.clone(),
        })
        .collect::<Vec<_>>();
    let request = build_snapshot_write_request(
        table,
        &column_descriptors,
        &parsed_rows,
        base_generation,
        "background_merge",
        &codec,
    )?;

    writer.replace_snapshot(&request)?;

    Ok(parsed_rows.len())
}

fn parse_live_rows(live_rows: &[crate::pg::catalog::LiveRow]) -> Result<Vec<RowVersion>> {
    let mut parsed_rows = Vec::with_capacity(live_rows.len());
    for live_row in live_rows {
        let row = serde_json::from_str::<Value>(&live_row.row_json)
            .with_context(|| format!("failed to parse row JSON for pk {}", live_row.pk_text))?;
        parsed_rows.push(RowVersion {
            pk_text: live_row.pk_text.clone(),
            row_json: row,
        });
    }
    Ok(parsed_rows)
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
