use crate::pg::catalog::{
    enqueue_backfill, enqueue_merge, install_capture_trigger, relation_identity,
    reset_sidecar_state, upsert_table_config,
};
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
