use crate::core::file_layout::incoming_dir_name;
use crate::core::interface::ScanRequest;
use crate::core::pk::PkCompareMode;
use crate::pg::catalog::{
    create_insert_buffer, enqueue_backfill, enqueue_merge, install_capture_trigger,
    load_active_insert_buffer, load_table_config, relation_identity, reset_sidecar_state,
    resolve_pk_compare_mode, resolve_projection, update_insert_buffer_after_append,
    upsert_table_config,
};
use crate::pg::scan::{execute_scan, plan_scan};
use crate::pg::storage::{
    insert_buffer_absolute_path, reset_table_storage, resolve_storage_root,
};
use anyhow::{Result, anyhow, bail};
use pgrx::JsonB;
use pgrx::datum::JsonB as DatumJsonB;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;

#[pg_extern]
fn pghouse_register_table(
    table_name: &str,
    pk_column: &str,
    granule_rows: default!(i32, 8192),
    compression: default!(String, "'zstd'"),
    storage_path: default!(String, "''"),
    insert_buffer_rows: default!(i32, 8192),
    insert_buffer_bytes: default!(i64, 8_388_608),
    insert_flush_ms: default!(i32, 200),
    backfill_existing: default!(bool, true),
) -> JsonB {
    let response = register_table(
        table_name,
        pk_column,
        granule_rows,
        &compression,
        &storage_path,
        insert_buffer_rows,
        insert_buffer_bytes,
        insert_flush_ms,
        backfill_existing,
    )
    .unwrap_or_else(|error| error!("pghouse_register_table failed: {error:#}"));
    JsonB(response)
}

#[pg_extern]
fn pghouse_schedule_merge(table_name: &str, reason: default!(String, "'manual'")) -> bool {
    schedule_merge(table_name, &reason)
        .unwrap_or_else(|error| error!("pghouse_schedule_merge failed: {error:#}"))
}

#[pg_extern]
fn pghouse_stage_insert(table_oid: i64, row_json: DatumJsonB) -> bool {
    stage_insert(table_oid, row_json.0)
        .unwrap_or_else(|error| error!("pghouse_stage_insert failed: {error:#}"))
}

#[pg_extern]
fn pghouse_explain_scan(
    table_name: &str,
    projection_csv: default!(String, "''"),
    pk_min: default!(String, "''"),
    pk_max: default!(String, "''"),
    limit: default!(i64, 0),
    snapshot_generation: default!(i64, 0),
) -> JsonB {
    let request = build_scan_request(
        table_name,
        &projection_csv,
        &pk_min,
        &pk_max,
        limit,
        snapshot_generation,
    )
    .unwrap_or_else(|error| error!("pghouse_explain_scan failed: {error:#}"));
    let plan = plan_scan(&request)
        .unwrap_or_else(|error| error!("pghouse_explain_scan failed: {error:#}"));
    JsonB(serde_json::to_value(plan).unwrap())
}

#[pg_extern]
fn pghouse_scan_rows(
    table_name: &str,
    projection_csv: default!(String, "''"),
    pk_min: default!(String, "''"),
    pk_max: default!(String, "''"),
    limit: default!(i64, 0),
    snapshot_generation: default!(i64, 0),
) -> JsonB {
    let request = build_scan_request(
        table_name,
        &projection_csv,
        &pk_min,
        &pk_max,
        limit,
        snapshot_generation,
    )
    .unwrap_or_else(|error| error!("pghouse_scan_rows failed: {error:#}"));
    let plan =
        plan_scan(&request).unwrap_or_else(|error| error!("pghouse_scan_rows failed: {error:#}"));
    let rows =
        execute_scan(&plan).unwrap_or_else(|error| error!("pghouse_scan_rows failed: {error:#}"));
    JsonB(json!({
        "plan": plan,
        "rows": rows,
        "row_count": rows.len(),
    }))
}

fn register_table(
    table_name: &str,
    pk_column: &str,
    granule_rows: i32,
    compression: &str,
    storage_path: &str,
    insert_buffer_rows: i32,
    insert_buffer_bytes: i64,
    insert_flush_ms: i32,
    backfill_existing: bool,
) -> Result<serde_json::Value> {
    if granule_rows <= 0 {
        bail!("granule_rows must be positive");
    }
    if insert_buffer_rows <= 0 {
        bail!("insert_buffer_rows must be positive");
    }
    if insert_buffer_bytes <= 0 {
        bail!("insert_buffer_bytes must be positive");
    }
    if insert_flush_ms <= 0 {
        bail!("insert_flush_ms must be positive");
    }
    if compression != "zstd" {
        bail!("only zstd compression is supported in the current bootstrap");
    }

    let identity = relation_identity(table_name)?;
    let storage_root = resolve_storage_root(identity.table_oid, storage_path)?;
    let pk_compare = resolve_pk_compare_mode(identity.table_oid, pk_column)?;
    upsert_table_config(
        &identity,
        pk_column,
        pk_compare,
        granule_rows,
        compression,
        &storage_root,
        insert_buffer_rows,
        insert_buffer_bytes,
        insert_flush_ms,
    )?;
    install_capture_trigger(&identity)?;
    reset_sidecar_state(identity.table_oid)?;
    reset_table_storage(&storage_root)?;

    if backfill_existing {
        enqueue_backfill(&identity, pk_column)?;
    }

    Ok(json!({
        "table_oid": identity.table_oid,
        "schema": identity.schema_name,
        "table": identity.relname,
        "pk_column": pk_column,
        "pk_compare": match pk_compare {
            PkCompareMode::Lexical => "lexical",
            PkCompareMode::I64 => "i64",
        },
        "granule_rows": granule_rows,
        "compression": compression,
        "storage_root": storage_root,
        "insert_buffer_rows": insert_buffer_rows,
        "insert_buffer_bytes": insert_buffer_bytes,
        "insert_flush_ms": insert_flush_ms,
        "backfill_existing": backfill_existing,
    }))
}

fn schedule_merge(table_name: &str, reason: &str) -> Result<bool> {
    let identity = relation_identity(table_name)?;
    enqueue_merge(identity.table_oid, reason)?;
    Ok(true)
}

fn build_scan_request(
    table_name: &str,
    projection_csv: &str,
    pk_min: &str,
    pk_max: &str,
    limit: i64,
    snapshot_generation: i64,
) -> Result<ScanRequest> {
    let identity = relation_identity(table_name)?;
    let config = load_table_config(identity.table_oid)?;
    let requested_columns = parse_projection_csv(projection_csv);
    let projection = resolve_projection(identity.table_oid, &requested_columns)?;

    Ok(ScanRequest {
        table: (&config).into(),
        projection,
        pk_min: normalize_text_arg(pk_min),
        pk_max: normalize_text_arg(pk_max),
        limit: if limit > 0 {
            Some(usize::try_from(limit).unwrap_or(usize::MAX))
        } else {
            None
        },
        snapshot_generation: if snapshot_generation > 0 {
            Some(snapshot_generation)
        } else {
            None
        },
    })
}

fn parse_projection_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_text_arg(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn stage_insert(table_oid: i64, row_json: Value) -> Result<bool> {
    let config = load_table_config(table_oid)?;
    let pk_text = row_json
        .get(&config.pk_column)
        .map(pk_value_to_text)
        .transpose()?
        .ok_or_else(|| anyhow!("missing PK column {} in staged insert", config.pk_column))?;

    let mut buffer = if let Some(buffer) = load_active_insert_buffer(table_oid)? {
        buffer
    } else {
        let storage_path = format!(
            "{}/{}_{}",
            incoming_dir_name(),
            config.table_oid,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or_default()
        );
        create_insert_buffer(table_oid, &storage_path, config.insert_flush_ms)?
    };

    let absolute_path = insert_buffer_absolute_path(&config.storage_root, &buffer.storage_path);
    let bytes_written = crate::core::file_layout::append_json_line(
        &absolute_path,
        &json!({
            "pk_text": pk_text,
            "row_json": row_json,
        }),
    )?;
    let new_row_count = buffer.row_count + 1;
    let new_byte_count = buffer.byte_count + i64::try_from(bytes_written).unwrap_or(i64::MAX);
    let should_seal =
        new_row_count >= config.insert_buffer_rows || new_byte_count >= config.insert_buffer_bytes;
    update_insert_buffer_after_append(
        buffer.id,
        1,
        i64::try_from(bytes_written).unwrap_or(i64::MAX),
        should_seal,
    )?;
    buffer.row_count = new_row_count;
    buffer.byte_count = new_byte_count;

    Ok(true)
}

fn pk_value_to_text(value: &Value) -> Result<String> {
    let text = match value {
        Value::Null => bail!("PK value cannot be NULL"),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        other => serde_json::to_string(other)?,
    };
    Ok(text)
}
