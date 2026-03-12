use crate::core::interface::ScanRequest;
use crate::pg::catalog::{
    enqueue_backfill, enqueue_merge, install_capture_trigger, load_table_config, relation_identity,
    reset_sidecar_state, resolve_projection, upsert_table_config,
};
use crate::pg::scan::{execute_scan, plan_scan};
use crate::pg::storage::{reset_table_storage, resolve_storage_root};
use anyhow::{Result, bail};
use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::json;

#[pg_extern]
fn pghouse_register_table(
    table_name: &str,
    pk_column: &str,
    granule_rows: default!(i32, 8192),
    compression: default!(String, "'zstd'"),
    storage_path: default!(String, "''"),
    backfill_existing: default!(bool, true),
) -> JsonB {
    let response = register_table(
        table_name,
        pk_column,
        granule_rows,
        &compression,
        &storage_path,
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
    backfill_existing: bool,
) -> Result<serde_json::Value> {
    if granule_rows <= 0 {
        bail!("granule_rows must be positive");
    }
    if compression != "zstd" {
        bail!("only zstd compression is supported in the current bootstrap");
    }

    let identity = relation_identity(table_name)?;
    let storage_root = resolve_storage_root(identity.table_oid, storage_path)?;
    upsert_table_config(
        &identity,
        pk_column,
        granule_rows,
        compression,
        &storage_root,
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
        "granule_rows": granule_rows,
        "compression": compression,
        "storage_root": storage_root,
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
