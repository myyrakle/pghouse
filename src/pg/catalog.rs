use crate::core::interface::{ColumnDescriptor, GranuleRef, TableDescriptor};
use crate::core::pk::{PkCompareMode, compare_pk_text};
use anyhow::{Context, Result, anyhow, bail};
use pgrx::prelude::*;
use pgrx::spi::{quote_identifier, quote_qualified_identifier};
use pgrx::{Spi, datum::DatumWithOid};

#[derive(Debug, Clone)]
pub(crate) struct RelationIdentity {
    pub(crate) table_oid: i64,
    pub(crate) schema_name: String,
    pub(crate) relname: String,
    pub(crate) access_method: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TableConfig {
    pub(crate) table_oid: i64,
    pub(crate) schema_name: String,
    pub(crate) relname: String,
    pub(crate) pk_column: String,
    pub(crate) pk_compare: PkCompareMode,
    pub(crate) granule_rows: i32,
    pub(crate) compression: String,
    pub(crate) storage_root: String,
    pub(crate) insert_buffer_rows: i32,
    pub(crate) insert_buffer_bytes: i64,
    pub(crate) insert_flush_ms: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingMutation {
    pub(crate) id: i64,
    pub(crate) op: String,
    pub(crate) pk_text: String,
    pub(crate) row_json: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveRow {
    pub(crate) pk_text: String,
    pub(crate) row_json: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnSpec {
    pub(crate) attnum: i32,
    pub(crate) attname: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InsertBuffer {
    pub(crate) id: i64,
    pub(crate) storage_path: String,
    pub(crate) row_count: i32,
    pub(crate) byte_count: i64,
}

impl From<&TableConfig> for TableDescriptor {
    fn from(value: &TableConfig) -> Self {
        Self {
            table_oid: value.table_oid,
            schema_name: value.schema_name.clone(),
            table_name: value.relname.clone(),
            pk_column: value.pk_column.clone(),
            pk_compare: value.pk_compare,
            granule_rows: usize::try_from(value.granule_rows.max(1)).unwrap_or(1),
            compression: value.compression.clone(),
            storage_root: value.storage_root.clone(),
        }
    }
}

pgrx::extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS pghouse;

    CREATE TABLE pghouse.tables (
        table_oid oid PRIMARY KEY,
        table_name regclass NOT NULL UNIQUE,
        pk_column name NOT NULL,
        pk_compare text NOT NULL CHECK (pk_compare IN ('lexical', 'i64')),
        granule_rows integer NOT NULL CHECK (granule_rows > 0),
        compression text NOT NULL CHECK (compression IN ('zstd')),
        storage_root text NOT NULL,
        insert_buffer_rows integer NOT NULL CHECK (insert_buffer_rows > 0),
        insert_buffer_bytes bigint NOT NULL CHECK (insert_buffer_bytes > 0),
        insert_flush_ms integer NOT NULL CHECK (insert_flush_ms > 0),
        registered_at timestamptz NOT NULL DEFAULT now(),
        last_flush_at timestamptz,
        last_merge_at timestamptz
    );

    CREATE TABLE pghouse.insert_buffers (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        storage_path text NOT NULL,
        row_count integer NOT NULL DEFAULT 0,
        byte_count bigint NOT NULL DEFAULT 0,
        created_at timestamptz NOT NULL DEFAULT now(),
        flush_after timestamptz NOT NULL,
        sealed_at timestamptz,
        flush_started_at timestamptz,
        flushed_at timestamptz
    );
    CREATE INDEX pghouse_insert_buffers_ready_idx
        ON pghouse.insert_buffers (table_oid, flushed_at, flush_started_at, flush_after, id);

    CREATE TABLE pghouse.pending_rows (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        op text NOT NULL CHECK (op IN ('update', 'delete')),
        pk_text text NOT NULL,
        row_json jsonb,
        queued_at timestamptz NOT NULL DEFAULT now(),
        apply_after timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz
    );
    CREATE INDEX pghouse_pending_rows_ready_idx
        ON pghouse.pending_rows (table_oid, processed_at, apply_after, id);

    CREATE TABLE pghouse.row_versions (
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        pk_text text NOT NULL,
        row_json jsonb,
        deleted boolean NOT NULL DEFAULT false,
        dirty boolean NOT NULL DEFAULT true,
        last_mutation_id bigint NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (table_oid, pk_text)
    );
    CREATE INDEX pghouse_row_versions_dirty_idx
        ON pghouse.row_versions (table_oid, dirty, deleted, pk_text);

    CREATE TABLE pghouse.granules (
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        generation bigint NOT NULL,
        row_count integer NOT NULL,
        pk_min text,
        pk_max text,
        manifest_path text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (table_oid, generation)
    );

    CREATE TABLE pghouse.merge_queue (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        reason text NOT NULL,
        requested_at timestamptz NOT NULL DEFAULT now(),
        started_at timestamptz,
        finished_at timestamptz,
        error_text text
    );
    CREATE INDEX pghouse_merge_queue_pending_idx
        ON pghouse.merge_queue (table_oid, finished_at, requested_at);

    CREATE OR REPLACE FUNCTION pghouse.capture_dml()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        meta pghouse.tables%ROWTYPE;
        payload jsonb;
        pk_value text;
        visible_at timestamptz := now();
    BEGIN
        SELECT *
        INTO meta
        FROM pghouse.tables
        WHERE table_oid = TG_RELID;

        IF NOT FOUND THEN
            RETURN COALESCE(NEW, OLD);
        END IF;

        payload := CASE TG_OP
            WHEN 'DELETE' THEN to_jsonb(OLD)
            ELSE to_jsonb(NEW)
        END;

        pk_value := payload ->> meta.pk_column;
        IF pk_value IS NULL THEN
            RAISE EXCEPTION 'pghouse: PK column % is NULL in %', meta.pk_column, TG_RELID::regclass;
        END IF;

        IF TG_OP IN ('UPDATE', 'DELETE') THEN
            visible_at := now() + interval '1 second';
        END IF;

        IF TG_OP = 'INSERT' THEN
            PERFORM pghouse_stage_insert(TG_RELID::bigint, payload);
            RETURN NEW;
        END IF;

        INSERT INTO pghouse.pending_rows (table_oid, op, pk_text, row_json, apply_after)
        VALUES (TG_RELID, lower(TG_OP), pk_value, CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE payload END, visible_at);

        INSERT INTO pghouse.merge_queue (table_oid, reason)
        VALUES (TG_RELID, lower(TG_OP) || '_captured');

        RETURN COALESCE(NEW, OLD);
    END;
    $$;
    "#,
    name = "bootstrap_pghouse_catalog",
    finalize
);

pub(crate) fn relation_identity(table_name: &str) -> anyhow::Result<RelationIdentity> {
    let args = [DatumWithOid::from(table_name)];

    let maybe_identity = Spi::connect(|client| -> Result<Option<RelationIdentity>, spi::Error> {
        let mut rows = client.select(
            r#"
            SELECT
                c.oid::bigint AS table_oid,
                n.nspname::text AS schema_name,
                c.relname::text AS relname,
                COALESCE(am.amname::text, '') AS access_method
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_am am ON am.oid = c.relam
            WHERE c.oid = to_regclass($1)
            "#,
            Some(1),
            &args,
        )?;

        if let Some(row) = rows.next() {
            Ok(Some(RelationIdentity {
                table_oid: row.get_by_name::<i64, _>("table_oid")?.unwrap_or_default(),
                schema_name: row
                    .get_by_name::<String, _>("schema_name")?
                    .unwrap_or_default(),
                relname: row.get_by_name::<String, _>("relname")?.unwrap_or_default(),
                access_method: row
                    .get_by_name::<String, _>("access_method")?
                    .unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    })?;

    maybe_identity
        .ok_or_else(|| anyhow!("relation {table_name} does not exist"))
        .and_then(|identity| {
            if identity.access_method != "pghouse" {
                bail!(
                    "relation {}.{} uses access method {}, expected pghouse",
                    identity.schema_name,
                    identity.relname,
                    identity.access_method
                );
            }
            Ok(identity)
        })
}

pub(crate) fn upsert_table_config(
    identity: &RelationIdentity,
    pk_column: &str,
    pk_compare: PkCompareMode,
    granule_rows: i32,
    compression: &str,
    storage_root: &str,
    insert_buffer_rows: i32,
    insert_buffer_bytes: i64,
    insert_flush_ms: i32,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(identity.table_oid),
        DatumWithOid::from(identity.table_oid),
        DatumWithOid::from(pk_column),
        DatumWithOid::from(pk_compare.as_sql_label()),
        DatumWithOid::from(granule_rows),
        DatumWithOid::from(compression),
        DatumWithOid::from(storage_root),
        DatumWithOid::from(insert_buffer_rows),
        DatumWithOid::from(insert_buffer_bytes),
        DatumWithOid::from(insert_flush_ms),
    ];

    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.tables (
            table_oid,
            table_name,
            pk_column,
            pk_compare,
            granule_rows,
            compression,
            storage_root,
            insert_buffer_rows,
            insert_buffer_bytes,
            insert_flush_ms
        )
        VALUES (
            $1::oid,
            $2::oid::regclass,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10
        )
        ON CONFLICT (table_oid)
        DO UPDATE SET
            pk_column = EXCLUDED.pk_column,
            pk_compare = EXCLUDED.pk_compare,
            granule_rows = EXCLUDED.granule_rows,
            compression = EXCLUDED.compression,
            storage_root = EXCLUDED.storage_root,
            insert_buffer_rows = EXCLUDED.insert_buffer_rows,
            insert_buffer_bytes = EXCLUDED.insert_buffer_bytes,
            insert_flush_ms = EXCLUDED.insert_flush_ms
        "#,
        &args,
    )?;

    Ok(())
}

pub(crate) fn reset_sidecar_state(table_oid: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid)];

    for statement in [
        "DELETE FROM pghouse.insert_buffers WHERE table_oid = $1::oid",
        "DELETE FROM pghouse.pending_rows WHERE table_oid = $1::oid",
        "DELETE FROM pghouse.row_versions WHERE table_oid = $1::oid",
        "DELETE FROM pghouse.merge_queue WHERE table_oid = $1::oid",
        "DELETE FROM pghouse.granules WHERE table_oid = $1::oid",
    ] {
        Spi::run_with_args(statement, &args)?;
    }

    Ok(())
}

pub(crate) fn install_capture_trigger(identity: &RelationIdentity) -> anyhow::Result<()> {
    let trigger_name = quote_identifier("pghouse_capture_dml");
    let qualified_table = quote_qualified_identifier(&identity.schema_name, &identity.relname);

    Spi::run(&format!(
        "DROP TRIGGER IF EXISTS {trigger_name} ON {qualified_table}"
    ))?;
    Spi::run(&format!(
        "CREATE TRIGGER {trigger_name} \
         AFTER INSERT OR UPDATE OR DELETE ON {qualified_table} \
         FOR EACH ROW EXECUTE FUNCTION pghouse.capture_dml()"
    ))?;

    Ok(())
}

pub(crate) fn enqueue_backfill(identity: &RelationIdentity, pk_column: &str) -> anyhow::Result<()> {
    let qualified_table = quote_qualified_identifier(&identity.schema_name, &identity.relname);
    let args = [
        DatumWithOid::from(identity.table_oid),
        DatumWithOid::from(pk_column),
    ];

    let query = format!(
        r#"
        SELECT pghouse_stage_insert(
            $1::bigint,
            to_jsonb(src)
        )
        FROM {qualified_table} AS src
        "#
    );
    Spi::run_with_args(&query, &args)?;

    Ok(())
}

pub(crate) fn enqueue_merge(table_oid: i64, reason: &str) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid), DatumWithOid::from(reason)];
    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.merge_queue (table_oid, reason)
        VALUES ($1::oid, $2)
        "#,
        &args,
    )?;
    Ok(())
}

pub(crate) fn load_table_config(table_oid: i64) -> anyhow::Result<TableConfig> {
    let args = [DatumWithOid::from(table_oid)];

    let maybe_config = Spi::connect(|client| -> Result<Option<TableConfig>, spi::Error> {
        let mut rows = client.select(
            r#"
            SELECT
                t.table_oid::bigint AS table_oid,
                n.nspname::text AS schema_name,
                c.relname::text AS relname,
                t.pk_column::text AS pk_column,
                t.pk_compare::text AS pk_compare,
                t.granule_rows AS granule_rows,
                t.compression::text AS compression,
                t.storage_root::text AS storage_root,
                t.insert_buffer_rows AS insert_buffer_rows,
                t.insert_buffer_bytes::bigint AS insert_buffer_bytes,
                t.insert_flush_ms AS insert_flush_ms
            FROM pghouse.tables t
            JOIN pg_class c ON c.oid = t.table_oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE t.table_oid = $1::oid
            "#,
            Some(1),
            &args,
        )?;

        if let Some(row) = rows.next() {
            Ok(Some(TableConfig {
                table_oid: row.get_by_name::<i64, _>("table_oid")?.unwrap_or_default(),
                schema_name: row
                    .get_by_name::<String, _>("schema_name")?
                    .unwrap_or_default(),
                relname: row.get_by_name::<String, _>("relname")?.unwrap_or_default(),
                pk_column: row
                    .get_by_name::<String, _>("pk_column")?
                    .unwrap_or_default(),
                pk_compare: PkCompareMode::from_sql_label(
                    &row.get_by_name::<String, _>("pk_compare")?.unwrap_or_default(),
                ),
                granule_rows: row
                    .get_by_name::<i32, _>("granule_rows")?
                    .unwrap_or_default(),
                compression: row
                    .get_by_name::<String, _>("compression")?
                    .unwrap_or_default(),
                storage_root: row
                    .get_by_name::<String, _>("storage_root")?
                    .unwrap_or_default(),
                insert_buffer_rows: row
                    .get_by_name::<i32, _>("insert_buffer_rows")?
                    .unwrap_or_default(),
                insert_buffer_bytes: row
                    .get_by_name::<i64, _>("insert_buffer_bytes")?
                    .unwrap_or_default(),
                insert_flush_ms: row
                    .get_by_name::<i32, _>("insert_flush_ms")?
                    .unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    })?;

    maybe_config.ok_or_else(|| anyhow!("pghouse metadata missing for table oid {table_oid}"))
}

pub(crate) fn load_candidate_tables(max_tables: i32) -> anyhow::Result<Vec<i64>> {
    let args = [DatumWithOid::from(max_tables)];

    let table_oids = Spi::connect(|client| -> Result<Vec<i64>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT DISTINCT table_oid::bigint
            FROM (
                SELECT table_oid
                FROM pghouse.insert_buffers
                WHERE flushed_at IS NULL
                  AND flush_started_at IS NULL
                  AND (sealed_at IS NOT NULL OR flush_after <= now())
                UNION ALL
                SELECT table_oid
                FROM pghouse.pending_rows
                WHERE processed_at IS NULL
                  AND apply_after <= now()
                UNION ALL
                SELECT table_oid
                FROM pghouse.merge_queue
                WHERE finished_at IS NULL
            ) candidates
            ORDER BY table_oid
            LIMIT $1
            "#,
            None,
            &args,
        )?;

        let mut table_oids = Vec::new();
        for row in rows {
            if let Some(table_oid) = row.get_by_name("table_oid")? {
                table_oids.push(table_oid);
            }
        }
        Ok(table_oids)
    })?;

    Ok(table_oids)
}

pub(crate) fn try_acquire_table_maintenance_lock(table_oid: i64) -> anyhow::Result<bool> {
    let args = [DatumWithOid::from(table_oid)];
    Ok(Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_lock($1::bigint)",
        &args,
    )?
    .unwrap_or(false))
}

pub(crate) fn release_table_maintenance_lock(table_oid: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid)];
    let released = Spi::get_one_with_args::<bool>(
        "SELECT pg_advisory_unlock($1::bigint)",
        &args,
    )?
    .unwrap_or(false);

    if !released {
        bail!("failed to release maintenance lock for table oid {table_oid}");
    }

    Ok(())
}

pub(crate) fn load_pending_rows(
    table_oid: i64,
    limit: i32,
) -> anyhow::Result<Vec<PendingMutation>> {
    let args = [DatumWithOid::from(table_oid), DatumWithOid::from(limit)];

    let pending_rows = Spi::connect(|client| -> Result<Vec<PendingMutation>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT
                id::bigint AS id,
                op::text AS op,
                pk_text::text AS pk_text,
                row_json::text AS row_json
            FROM pghouse.pending_rows
            WHERE table_oid = $1::oid
              AND processed_at IS NULL
              AND apply_after <= now()
            ORDER BY id
            LIMIT $2
            "#,
            None,
            &args,
        )?;

        let mut pending = Vec::new();
        for row in rows {
            pending.push(PendingMutation {
                id: row.get_by_name::<i64, _>("id")?.unwrap_or_default(),
                op: row.get_by_name::<String, _>("op")?.unwrap_or_default(),
                pk_text: row.get_by_name::<String, _>("pk_text")?.unwrap_or_default(),
                row_json: row.get_by_name::<String, _>("row_json")?,
            });
        }
        Ok(pending)
    })?;

    Ok(pending_rows)
}

pub(crate) fn load_live_rows(
    table_oid: i64,
    pk_compare: PkCompareMode,
) -> anyhow::Result<Vec<LiveRow>> {
    let args = [DatumWithOid::from(table_oid)];

    let mut live_rows = Spi::connect(|client| -> Result<Vec<LiveRow>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT pk_text::text AS pk_text, row_json::text AS row_json
            FROM pghouse.row_versions
            WHERE table_oid = $1::oid
              AND NOT deleted
            "#,
            None,
            &args,
        )?;

        let mut live = Vec::new();
        for row in rows {
            if let (Some(pk_text), Some(row_json)) =
                (row.get_by_name("pk_text")?, row.get_by_name("row_json")?)
            {
                live.push(LiveRow { pk_text, row_json });
            }
        }
        Ok(live)
    })?;

    live_rows.sort_by(|left, right| compare_pk_text(pk_compare, &left.pk_text, &right.pk_text));
    Ok(live_rows)
}

pub(crate) fn resolve_pk_compare_mode(
    table_oid: i64,
    pk_column: &str,
) -> anyhow::Result<PkCompareMode> {
    let args = [DatumWithOid::from(table_oid), DatumWithOid::from(pk_column)];
    let typname = Spi::get_one_with_args::<String>(
        r#"
        SELECT t.typname::text
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = $1::oid
          AND a.attname = $2::name
          AND a.attnum > 0
          AND NOT a.attisdropped
        "#,
        &args,
    )?
    .ok_or_else(|| anyhow!("PK column {pk_column} does not exist on table oid {table_oid}"))?;

    Ok(match typname.as_str() {
        "int2" | "int4" | "int8" | "oid" => PkCompareMode::I64,
        _ => PkCompareMode::Lexical,
    })
}

pub(crate) fn load_column_specs(table_oid: i64) -> anyhow::Result<Vec<ColumnSpec>> {
    let args = [DatumWithOid::from(table_oid)];

    let columns = Spi::connect(|client| -> Result<Vec<ColumnSpec>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT attnum AS attnum, attname::text AS attname
            FROM pg_attribute
            WHERE attrelid = $1::oid
              AND attnum > 0
              AND NOT attisdropped
            ORDER BY attnum
            "#,
            None,
            &args,
        )?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(ColumnSpec {
                attnum: row.get_by_name::<i32, _>("attnum")?.unwrap_or_default(),
                attname: row.get_by_name::<String, _>("attname")?.unwrap_or_default(),
            });
        }
        Ok(columns)
    })?;

    Ok(columns)
}

pub(crate) fn resolve_projection(
    table_oid: i64,
    requested_columns: &[String],
) -> anyhow::Result<Vec<ColumnDescriptor>> {
    let available_columns = load_column_specs(table_oid)?;
    if requested_columns.is_empty() {
        return Ok(available_columns
            .into_iter()
            .map(|column| ColumnDescriptor {
                ordinal: column.attnum,
                name: column.attname,
            })
            .collect());
    }

    let mut projection = Vec::with_capacity(requested_columns.len());
    for requested_column in requested_columns {
        let column = available_columns
            .iter()
            .find(|column| column.attname == *requested_column)
            .ok_or_else(|| {
                anyhow!("column {requested_column} does not exist on table oid {table_oid}")
            })?;
        projection.push(ColumnDescriptor {
            ordinal: column.attnum,
            name: column.attname.clone(),
        });
    }

    Ok(projection)
}

pub(crate) fn load_scan_granules(
    table_oid: i64,
    snapshot_generation: Option<i64>,
) -> anyhow::Result<Vec<GranuleRef>> {
    let args = [
        DatumWithOid::from(table_oid),
        snapshot_generation
            .map(DatumWithOid::from)
            .unwrap_or_else(DatumWithOid::null::<i64>),
    ];

    let granules = Spi::connect(|client| -> Result<Vec<GranuleRef>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT
                table_oid::bigint AS table_oid,
                generation::bigint AS generation,
                row_count AS row_count,
                pk_min::text AS pk_min,
                pk_max::text AS pk_max,
                manifest_path::text AS manifest_path
            FROM pghouse.granules
            WHERE table_oid = $1::oid
              AND ($2 IS NULL OR generation <= $2)
            ORDER BY generation
            "#,
            None,
            &args,
        )?;

        let mut granules = Vec::new();
        for row in rows {
            granules.push(GranuleRef {
                table_oid: row.get_by_name::<i64, _>("table_oid")?.unwrap_or_default(),
                generation: row.get_by_name::<i64, _>("generation")?.unwrap_or_default(),
                row_count: row.get_by_name::<i32, _>("row_count")?.unwrap_or_default(),
                pk_min: row.get_by_name::<String, _>("pk_min")?,
                pk_max: row.get_by_name::<String, _>("pk_max")?,
                manifest_path: row
                    .get_by_name::<String, _>("manifest_path")?
                    .unwrap_or_default(),
            });
        }
        Ok(granules)
    })?;

    Ok(granules)
}

pub(crate) fn load_active_insert_buffer(table_oid: i64) -> anyhow::Result<Option<InsertBuffer>> {
    let args = [DatumWithOid::from(table_oid)];

    let buffer = Spi::connect(|client| -> Result<Option<InsertBuffer>, spi::Error> {
        let mut rows = client.select(
            r#"
            SELECT
                id::bigint AS id,
                storage_path::text AS storage_path,
                row_count AS row_count,
                byte_count::bigint AS byte_count
            FROM pghouse.insert_buffers
            WHERE table_oid = $1::oid
              AND sealed_at IS NULL
              AND flushed_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            FOR UPDATE
            "#,
            Some(1),
            &args,
        )?;

        if let Some(row) = rows.next() {
            Ok(Some(InsertBuffer {
                id: row.get_by_name::<i64, _>("id")?.unwrap_or_default(),
                storage_path: row
                    .get_by_name::<String, _>("storage_path")?
                    .unwrap_or_default(),
                row_count: row.get_by_name::<i32, _>("row_count")?.unwrap_or_default(),
                byte_count: row.get_by_name::<i64, _>("byte_count")?.unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    })?;

    Ok(buffer)
}

pub(crate) fn create_insert_buffer(
    table_oid: i64,
    storage_path: &str,
    flush_after_ms: i32,
) -> anyhow::Result<InsertBuffer> {
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(storage_path),
        DatumWithOid::from(flush_after_ms),
    ];

    let id = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pghouse.insert_buffers (table_oid, storage_path, flush_after)
        VALUES (
            $1::oid,
            $2,
            now() + make_interval(secs => 0) + ($3::double precision / 1000.0) * interval '1 second'
        )
        RETURNING id::bigint
        "#,
        &args,
    )?
    .context("failed to create insert buffer")?;

    Ok(InsertBuffer {
        id,
        storage_path: storage_path.to_string(),
        row_count: 0,
        byte_count: 0,
    })
}

pub(crate) fn update_insert_buffer_after_append(
    buffer_id: i64,
    row_delta: i32,
    byte_delta: i64,
    seal: bool,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(buffer_id),
        DatumWithOid::from(row_delta),
        DatumWithOid::from(byte_delta),
        DatumWithOid::from(seal),
    ];

    Spi::run_with_args(
        r#"
        UPDATE pghouse.insert_buffers
        SET row_count = row_count + $2,
            byte_count = byte_count + $3,
            sealed_at = CASE
                WHEN $4 THEN COALESCE(sealed_at, now())
                ELSE sealed_at
            END
        WHERE id = $1
        "#,
        &args,
    )?;

    Ok(())
}

pub(crate) fn claim_ready_insert_buffers(
    table_oid: i64,
    limit: i32,
) -> anyhow::Result<Vec<InsertBuffer>> {
    let args = [DatumWithOid::from(table_oid), DatumWithOid::from(limit)];

    let buffers = Spi::connect(|client| -> Result<Vec<InsertBuffer>, spi::Error> {
        let rows = client.select(
            r#"
            WITH ready AS (
                SELECT id
                FROM pghouse.insert_buffers
                WHERE table_oid = $1::oid
                  AND flushed_at IS NULL
                  AND flush_started_at IS NULL
                  AND (sealed_at IS NOT NULL OR flush_after <= now())
                ORDER BY id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            claimed AS (
                UPDATE pghouse.insert_buffers b
                SET flush_started_at = now(),
                    sealed_at = COALESCE(sealed_at, now())
                FROM ready
                WHERE b.id = ready.id
                RETURNING b.id::bigint AS id,
                          b.storage_path::text AS storage_path,
                          b.row_count AS row_count,
                          b.byte_count::bigint AS byte_count
            )
            SELECT * FROM claimed ORDER BY id
            "#,
            None,
            &args,
        )?;

        let mut buffers = Vec::new();
        for row in rows {
            buffers.push(InsertBuffer {
                id: row.get_by_name::<i64, _>("id")?.unwrap_or_default(),
                storage_path: row
                    .get_by_name::<String, _>("storage_path")?
                    .unwrap_or_default(),
                row_count: row.get_by_name::<i32, _>("row_count")?.unwrap_or_default(),
                byte_count: row.get_by_name::<i64, _>("byte_count")?.unwrap_or_default(),
            });
        }
        Ok(buffers)
    })?;

    Ok(buffers)
}

pub(crate) fn load_visible_insert_buffers(table_oid: i64) -> anyhow::Result<Vec<InsertBuffer>> {
    let args = [DatumWithOid::from(table_oid)];

    let buffers = Spi::connect(|client| -> Result<Vec<InsertBuffer>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT
                id::bigint AS id,
                storage_path::text AS storage_path,
                row_count AS row_count,
                byte_count::bigint AS byte_count
            FROM pghouse.insert_buffers
            WHERE table_oid = $1::oid
              AND flushed_at IS NULL
              AND flush_started_at IS NULL
            ORDER BY id
            "#,
            None,
            &args,
        )?;

        let mut buffers = Vec::new();
        for row in rows {
            buffers.push(InsertBuffer {
                id: row.get_by_name::<i64, _>("id")?.unwrap_or_default(),
                storage_path: row
                    .get_by_name::<String, _>("storage_path")?
                    .unwrap_or_default(),
                row_count: row.get_by_name::<i32, _>("row_count")?.unwrap_or_default(),
                byte_count: row.get_by_name::<i64, _>("byte_count")?.unwrap_or_default(),
            });
        }
        Ok(buffers)
    })?;

    Ok(buffers)
}

pub(crate) fn mark_insert_buffer_flushed(buffer_id: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(buffer_id)];
    Spi::run_with_args(
        r#"
        UPDATE pghouse.insert_buffers
        SET flushed_at = now()
        WHERE id = $1
        "#,
        &args,
    )?;
    Ok(())
}

pub(crate) fn release_insert_buffer_claim(buffer_id: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(buffer_id)];
    Spi::run_with_args(
        r#"
        UPDATE pghouse.insert_buffers
        SET flush_started_at = NULL
        WHERE id = $1
          AND flushed_at IS NULL
        "#,
        &args,
    )?;
    Ok(())
}

pub(crate) fn pending_merge_count(table_oid: i64) -> anyhow::Result<i64> {
    let args = [DatumWithOid::from(table_oid)];
    let count = Spi::get_one_with_args::<i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM pghouse.merge_queue
        WHERE table_oid = $1::oid
          AND finished_at IS NULL
        "#,
        &args,
    )?
    .unwrap_or_default();

    Ok(count)
}

pub(crate) fn dirty_row_count(table_oid: i64) -> anyhow::Result<i64> {
    let args = [DatumWithOid::from(table_oid)];
    let count = Spi::get_one_with_args::<i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM pghouse.row_versions
        WHERE table_oid = $1::oid
          AND dirty
        "#,
        &args,
    )?
    .unwrap_or_default();

    Ok(count)
}

pub(crate) fn next_generation(table_oid: i64) -> anyhow::Result<i64> {
    let args = [DatumWithOid::from(table_oid)];
    let generation = Spi::get_one_with_args::<i64>(
        r#"
        SELECT COALESCE(MAX(generation), 0)::bigint + 1
        FROM pghouse.granules
        WHERE table_oid = $1::oid
        "#,
        &args,
    )?
    .unwrap_or(1);

    Ok(generation)
}

pub(crate) fn apply_mutation(mutation: &PendingMutation, table_oid: i64) -> anyhow::Result<()> {
    let table_oid_arg = DatumWithOid::from(table_oid);
    let pk_arg = DatumWithOid::from(mutation.pk_text.as_str());
    let mutation_id_arg = DatumWithOid::from(mutation.id);

    match mutation.op.as_str() {
        "insert" | "update" => {
            let row_json = mutation.row_json.as_deref().ok_or_else(|| {
                anyhow!(
                    "missing row_json for {} mutation {}",
                    mutation.op,
                    mutation.id
                )
            })?;
            let args = [
                table_oid_arg,
                pk_arg,
                DatumWithOid::from(row_json),
                mutation_id_arg,
            ];
            Spi::run_with_args(
                r#"
                INSERT INTO pghouse.row_versions (
                    table_oid,
                    pk_text,
                    row_json,
                    deleted,
                    dirty,
                    last_mutation_id
                )
                VALUES (
                    $1::oid,
                    $2,
                    $3::jsonb,
                    false,
                    true,
                    $4
                )
                ON CONFLICT (table_oid, pk_text)
                DO UPDATE SET
                    row_json = EXCLUDED.row_json,
                    deleted = false,
                    dirty = true,
                    last_mutation_id = EXCLUDED.last_mutation_id,
                    updated_at = now()
                "#,
                &args,
            )?;
        }
        "delete" => {
            let args = [table_oid_arg, pk_arg, mutation_id_arg];
            Spi::run_with_args(
                r#"
                INSERT INTO pghouse.row_versions (
                    table_oid,
                    pk_text,
                    row_json,
                    deleted,
                    dirty,
                    last_mutation_id
                )
                VALUES (
                    $1::oid,
                    $2,
                    NULL,
                    true,
                    true,
                    $3
                )
                ON CONFLICT (table_oid, pk_text)
                DO UPDATE SET
                    row_json = NULL,
                    deleted = true,
                    dirty = true,
                    last_mutation_id = EXCLUDED.last_mutation_id,
                    updated_at = now()
                "#,
                &args,
            )?;
        }
        other => bail!("unsupported mutation op {other}"),
    }

    let args = [DatumWithOid::from(mutation.id)];
    Spi::run_with_args(
        "UPDATE pghouse.pending_rows SET processed_at = now() WHERE id = $1",
        &args,
    )?;

    Ok(())
}

pub(crate) fn upsert_buffered_row_version(
    table_oid: i64,
    pk_text: &str,
    row_json: &str,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(pk_text),
        DatumWithOid::from(row_json),
    ];

    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.row_versions (
            table_oid,
            pk_text,
            row_json,
            deleted,
            dirty,
            last_mutation_id
        )
        VALUES (
            $1::oid,
            $2,
            $3::jsonb,
            false,
            false,
            0
        )
        ON CONFLICT (table_oid, pk_text)
        DO UPDATE SET
            row_json = EXCLUDED.row_json,
            deleted = false,
            dirty = false,
            updated_at = now()
        "#,
        &args,
    )?;

    Ok(())
}

pub(crate) fn insert_granule(
    table_oid: i64,
    generation: i64,
    row_count: i32,
    pk_min: Option<&str>,
    pk_max: Option<&str>,
    manifest_path: &str,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(generation),
        DatumWithOid::from(row_count),
        pk_min.map_or_else(|| DatumWithOid::null::<String>(), DatumWithOid::from),
        pk_max.map_or_else(|| DatumWithOid::null::<String>(), DatumWithOid::from),
        DatumWithOid::from(manifest_path),
    ];

    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.granules (
            table_oid,
            generation,
            row_count,
            pk_min,
            pk_max,
            manifest_path
        )
        VALUES (
            $1::oid,
            $2,
            $3,
            $4,
            $5,
            $6
        )
        "#,
        &args,
    )
    .context("failed to insert granule metadata")?;

    Ok(())
}

pub(crate) fn mark_merge_success(table_oid: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid)];

    for statement in [
        "UPDATE pghouse.tables SET last_merge_at = now(), last_flush_at = now() WHERE table_oid = $1::oid",
        "UPDATE pghouse.row_versions SET dirty = false WHERE table_oid = $1::oid",
        "UPDATE pghouse.merge_queue SET started_at = COALESCE(started_at, now()), finished_at = now(), error_text = NULL WHERE table_oid = $1::oid AND finished_at IS NULL",
    ] {
        Spi::run_with_args(statement, &args)?;
    }

    Ok(())
}

pub(crate) fn mark_flush_success(table_oid: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid)];
    Spi::run_with_args(
        r#"
        UPDATE pghouse.tables
        SET last_flush_at = now()
        WHERE table_oid = $1::oid
        "#,
        &args,
    )?;
    Ok(())
}

pub(crate) fn mark_merge_failure(table_oid: i64, error_text: &str) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(error_text),
    ];
    Spi::run_with_args(
        r#"
        UPDATE pghouse.merge_queue
        SET started_at = COALESCE(started_at, now()),
            error_text = $2
        WHERE table_oid = $1::oid
          AND finished_at IS NULL
        "#,
        &args,
    )?;
    Ok(())
}

pub(crate) fn delete_stale_granules(
    table_oid: i64,
    min_generation_to_keep: Option<i64>,
) -> anyhow::Result<()> {
    match min_generation_to_keep {
        Some(min_generation_to_keep) => {
            let args = [
                DatumWithOid::from(table_oid),
                DatumWithOid::from(min_generation_to_keep),
            ];

            Spi::run_with_args(
                r#"
                DELETE FROM pghouse.granules
                WHERE table_oid = $1::oid
                  AND generation < $2
                "#,
                &args,
            )?;
        }
        None => {
            let args = [DatumWithOid::from(table_oid)];
            Spi::run_with_args(
                r#"
                DELETE FROM pghouse.granules
                WHERE table_oid = $1::oid
                "#,
                &args,
            )?;
        }
    }

    Ok(())
}

pub(crate) fn delete_all_granules(table_oid: i64) -> anyhow::Result<()> {
    delete_stale_granules(table_oid, None)
}
