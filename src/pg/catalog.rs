use crate::core::interface::TableDescriptor;
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
    pub(crate) granule_rows: i32,
    pub(crate) compression: String,
    pub(crate) storage_root: String,
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

impl From<&TableConfig> for TableDescriptor {
    fn from(value: &TableConfig) -> Self {
        Self {
            table_oid: value.table_oid,
            schema_name: value.schema_name.clone(),
            table_name: value.relname.clone(),
            pk_column: value.pk_column.clone(),
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
        granule_rows integer NOT NULL CHECK (granule_rows > 0),
        compression text NOT NULL CHECK (compression IN ('zstd')),
        storage_root text NOT NULL,
        registered_at timestamptz NOT NULL DEFAULT now(),
        last_flush_at timestamptz,
        last_merge_at timestamptz
    );

    CREATE TABLE pghouse.pending_rows (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        op text NOT NULL CHECK (op IN ('insert', 'update', 'delete')),
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
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_oid oid NOT NULL REFERENCES pghouse.tables(table_oid) ON DELETE CASCADE,
        generation bigint NOT NULL,
        row_count integer NOT NULL,
        pk_min text,
        pk_max text,
        codec text NOT NULL,
        merge_reason text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX pghouse_granules_generation_idx
        ON pghouse.granules (table_oid, generation);

    CREATE TABLE pghouse.column_chunks (
        granule_id bigint NOT NULL REFERENCES pghouse.granules(id) ON DELETE CASCADE,
        column_name name NOT NULL,
        column_ordinal integer NOT NULL,
        compression text NOT NULL,
        row_count integer NOT NULL,
        uncompressed_bytes integer NOT NULL,
        compressed_bytes integer NOT NULL,
        storage_path text NOT NULL,
        PRIMARY KEY (granule_id, column_name)
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

        INSERT INTO pghouse.pending_rows (
            table_oid,
            op,
            pk_text,
            row_json,
            apply_after
        )
        VALUES (
            TG_RELID,
            lower(TG_OP),
            pk_value,
            CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE payload END,
            visible_at
        );

        IF TG_OP IN ('UPDATE', 'DELETE') THEN
            INSERT INTO pghouse.merge_queue (table_oid, reason)
            VALUES (TG_RELID, lower(TG_OP) || '_captured');
        END IF;

        RETURN COALESCE(NEW, OLD);
    END;
    $$;
    "#,
    name = "bootstrap_pghouse_catalog"
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
    granule_rows: i32,
    compression: &str,
    storage_root: &str,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(identity.table_oid),
        DatumWithOid::from(identity.table_oid),
        DatumWithOid::from(pk_column),
        DatumWithOid::from(granule_rows),
        DatumWithOid::from(compression),
        DatumWithOid::from(storage_root),
    ];

    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.tables (
            table_oid,
            table_name,
            pk_column,
            granule_rows,
            compression,
            storage_root
        )
        VALUES (
            $1::oid,
            $2::oid::regclass,
            $3,
            $4,
            $5,
            $6
        )
        ON CONFLICT (table_oid)
        DO UPDATE SET
            pk_column = EXCLUDED.pk_column,
            granule_rows = EXCLUDED.granule_rows,
            compression = EXCLUDED.compression,
            storage_root = EXCLUDED.storage_root
        "#,
        &args,
    )?;

    Ok(())
}

pub(crate) fn reset_sidecar_state(table_oid: i64) -> anyhow::Result<()> {
    let args = [DatumWithOid::from(table_oid)];

    for statement in [
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
        INSERT INTO pghouse.pending_rows (table_oid, op, pk_text, row_json)
        SELECT
            $1::oid,
            'insert',
            to_jsonb(src) ->> $2,
            to_jsonb(src)
        FROM {qualified_table} AS src
        "#
    );
    Spi::run_with_args(&query, &args)?;

    enqueue_merge(identity.table_oid, "bootstrap_backfill")?;

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
                t.granule_rows AS granule_rows,
                t.compression::text AS compression,
                t.storage_root::text AS storage_root
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
                granule_rows: row
                    .get_by_name::<i32, _>("granule_rows")?
                    .unwrap_or_default(),
                compression: row
                    .get_by_name::<String, _>("compression")?
                    .unwrap_or_default(),
                storage_root: row
                    .get_by_name::<String, _>("storage_root")?
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

pub(crate) fn load_live_rows(table_oid: i64) -> anyhow::Result<Vec<LiveRow>> {
    let args = [DatumWithOid::from(table_oid)];

    let live_rows = Spi::connect(|client| -> Result<Vec<LiveRow>, spi::Error> {
        let rows = client.select(
            r#"
            SELECT pk_text::text AS pk_text, row_json::text AS row_json
            FROM pghouse.row_versions
            WHERE table_oid = $1::oid
              AND NOT deleted
            ORDER BY pk_text
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

    Ok(live_rows)
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

pub(crate) fn insert_granule(
    table_oid: i64,
    generation: i64,
    row_count: i32,
    pk_min: Option<&str>,
    pk_max: Option<&str>,
    codec: &str,
    merge_reason: &str,
) -> anyhow::Result<i64> {
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(generation),
        DatumWithOid::from(row_count),
        pk_min.map_or_else(|| DatumWithOid::null::<String>(), DatumWithOid::from),
        pk_max.map_or_else(|| DatumWithOid::null::<String>(), DatumWithOid::from),
        DatumWithOid::from(codec),
        DatumWithOid::from(merge_reason),
    ];

    let granule_id = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pghouse.granules (
            table_oid,
            generation,
            row_count,
            pk_min,
            pk_max,
            codec,
            merge_reason
        )
        VALUES (
            $1::oid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7
        )
        RETURNING id::bigint
        "#,
        &args,
    )?
    .context("failed to insert granule")?;

    Ok(granule_id)
}

pub(crate) fn insert_column_chunk(
    granule_id: i64,
    column_name: &str,
    column_ordinal: i32,
    compression: &str,
    row_count: i32,
    uncompressed_bytes: i32,
    compressed_bytes: i32,
    storage_path: &str,
) -> anyhow::Result<()> {
    let args = [
        DatumWithOid::from(granule_id),
        DatumWithOid::from(column_name),
        DatumWithOid::from(column_ordinal),
        DatumWithOid::from(compression),
        DatumWithOid::from(row_count),
        DatumWithOid::from(uncompressed_bytes),
        DatumWithOid::from(compressed_bytes),
        DatumWithOid::from(storage_path),
    ];

    Spi::run_with_args(
        r#"
        INSERT INTO pghouse.column_chunks (
            granule_id,
            column_name,
            column_ordinal,
            compression,
            row_count,
            uncompressed_bytes,
            compressed_bytes,
            storage_path
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        &args,
    )?;

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
