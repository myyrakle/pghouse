#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark regular PostgreSQL table vs pghouse table."
    )
    parser.add_argument("--db", default=os.environ.get("PGDATABASE", "postgres"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    parser.add_argument("--host", default=os.environ.get("PGHOST", ""))
    parser.add_argument("--port", default=os.environ.get("PGPORT", ""))
    parser.add_argument("--rows", type=int, default=50_000_000)
    parser.add_argument("--batch-rows", type=int, default=1_000_000)
    parser.add_argument("--agg-repeats", type=int, default=3)
    parser.add_argument("--schema", default="public")
    parser.add_argument("--postgres-table", default="bench_events_postgres_50m")
    parser.add_argument("--pghouse-table", default="bench_events_pghouse_50m")
    parser.add_argument("--granule-rows", type=int, default=8192)
    parser.add_argument("--insert-buffer-rows", type=int, default=0)
    parser.add_argument("--insert-buffer-bytes", type=int, default=1_073_741_824)
    parser.add_argument("--insert-flush-ms", type=int, default=3_600_000)
    parser.add_argument("--storage-path", default="")
    parser.add_argument("--result-path", default="")
    parser.add_argument("--keep-existing", action="store_true")
    return parser.parse_args()


def psql_base_args(args: argparse.Namespace) -> list[str]:
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-U", args.user, "-d", args.db]
    if args.host:
        cmd += ["-h", args.host]
    if args.port:
        cmd += ["-p", args.port]
    return cmd


def run_psql(
    args: argparse.Namespace,
    sql: str,
    *,
    capture_output: bool = False,
    quiet: bool = False,
) -> str:
    cmd = psql_base_args(args)
    if quiet:
        cmd.append("-q")
    cmd += ["-c", sql]
    completed = subprocess.run(
        cmd,
        text=True,
        capture_output=capture_output,
        stdout=subprocess.DEVNULL if quiet and not capture_output else None,
        check=True,
    )
    return completed.stdout if capture_output else ""


def scalar(args: argparse.Namespace, sql: str) -> str:
    cmd = psql_base_args(args) + ["-Atqc", sql]
    completed = subprocess.run(cmd, text=True, capture_output=True, check=True)
    return completed.stdout.strip()


def measure_ms(fn, *fn_args, **fn_kwargs) -> int:
    started = time.perf_counter_ns()
    fn(*fn_args, **fn_kwargs)
    ended = time.perf_counter_ns()
    return (ended - started) // 1_000_000


def full_name(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def batch_insert_sql(table_name: str, start_id: int, end_id: int) -> str:
    return f"""
INSERT INTO {table_name}
SELECT
    gs AS id,
    timestamptz '2024-01-01 00:00:00+00' + ((gs % 31536000) * interval '1 second') AS occurred_at,
    ((gs * 17) % 10000000) + 1 AS customer_id,
    ((gs * 13) % 65536)::integer AS device_id,
    ((gs * 7) % 32)::smallint AS region,
    ((gs * 29) % 5000)::integer AS campaign_id,
    ((gs * 31) % 100000)::integer AS revenue_cents,
    (((gs * 97) % 100000)::double precision / 100.0) AS score
FROM generate_series({start_id}::bigint, {end_id}::bigint) AS gs;
""".strip()


def aggregate_sql(table_name: str) -> str:
    return f"""
SELECT
    region,
    campaign_id,
    COUNT(*) AS row_count,
    SUM(revenue_cents) AS revenue_sum,
    AVG(score) AS score_avg
FROM {table_name}
WHERE occurred_at >= timestamptz '2024-03-01 00:00:00+00'
  AND occurred_at < timestamptz '2024-09-01 00:00:00+00'
  AND region BETWEEN 3 AND 12
GROUP BY region, campaign_id
ORDER BY region, campaign_id;
""".strip()


def prepare_tables(args: argparse.Namespace, postgres_name: str, pghouse_name: str) -> None:
    drop_sql = ""
    if not args.keep_existing:
        drop_sql = f"""
DROP TABLE IF EXISTS {postgres_name};
DROP TABLE IF EXISTS {pghouse_name};
""".strip()
    insert_buffer_rows = args.insert_buffer_rows or args.batch_rows
    sql = f"""
CREATE EXTENSION IF NOT EXISTS pghouse;
{drop_sql}

CREATE TABLE IF NOT EXISTS {postgres_name} (
    id bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    customer_id bigint NOT NULL,
    device_id integer NOT NULL,
    region smallint NOT NULL,
    campaign_id integer NOT NULL,
    revenue_cents integer NOT NULL,
    score double precision NOT NULL
);

CREATE TABLE IF NOT EXISTS {pghouse_name} (
    id bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    customer_id bigint NOT NULL,
    device_id integer NOT NULL,
    region smallint NOT NULL,
    campaign_id integer NOT NULL,
    revenue_cents integer NOT NULL,
    score double precision NOT NULL
) USING pghouse;

SELECT pghouse_register_table(
    table_name => '{pghouse_name}',
    pk_column => 'id',
    granule_rows => {args.granule_rows},
    compression => 'zstd',
    storage_path => '{args.storage_path}',
    insert_buffer_rows => {insert_buffer_rows},
    insert_buffer_bytes => {args.insert_buffer_bytes},
    insert_flush_ms => {args.insert_flush_ms},
    backfill_existing => false
);
""".strip()
    run_psql(args, sql)


def load_regular_table(args: argparse.Namespace, table_name: str) -> None:
    batch_no = 0
    for start_id in range(1, args.rows + 1, args.batch_rows):
        end_id = min(start_id + args.batch_rows - 1, args.rows)
        batch_no += 1
        print(f"postgres batch {batch_no}: {start_id}..{end_id}", file=sys.stderr)
        run_psql(args, batch_insert_sql(table_name, start_id, end_id), quiet=True)


def load_pghouse_table(args: argparse.Namespace, table_name: str) -> None:
    batch_no = 0
    for start_id in range(1, args.rows + 1, args.batch_rows):
        end_id = min(start_id + args.batch_rows - 1, args.rows)
        batch_no += 1
        print(f"pghouse batch {batch_no}: {start_id}..{end_id}", file=sys.stderr)
        run_psql(
            args,
            "\n".join(
                [
                    batch_insert_sql(table_name, start_id, end_id),
                    "SELECT pghouse_run_maintenance_once(256, 65536);",
                ]
            ),
            quiet=True,
        )

    run_psql(
        args,
        f"""
UPDATE pghouse.insert_buffers
SET sealed_at = COALESCE(sealed_at, now())
WHERE table_oid = '{table_name}'::regclass
  AND flushed_at IS NULL;

SELECT pghouse_run_maintenance_once(4096, 65536);
""".strip(),
        quiet=True,
    )


def measure_aggregates(args: argparse.Namespace, label: str, table_name: str) -> list[int]:
    sql = aggregate_sql(table_name)
    times: list[int] = []
    for run_no in range(1, args.agg_repeats + 1):
        print(f"{label} aggregate run {run_no}/{args.agg_repeats}", file=sys.stderr)
        times.append(measure_ms(run_psql, args, sql, quiet=True))
    return times


def main() -> int:
    args = parse_args()
    postgres_name = full_name(args.schema, args.postgres_table)
    pghouse_name = full_name(args.schema, args.pghouse_table)
    result_path = (
        Path(args.result_path)
        if args.result_path
        else Path("bench/results") / f"postgres_vs_pghouse_{args.rows}.json"
    )

    print("Preparing benchmark tables", file=sys.stderr)
    prepare_tables(args, postgres_name, pghouse_name)

    print(f"Loading postgres table {postgres_name}", file=sys.stderr)
    postgres_insert_ms = measure_ms(load_regular_table, args, postgres_name)

    print(f"Loading pghouse table {pghouse_name}", file=sys.stderr)
    pghouse_insert_ms = measure_ms(load_pghouse_table, args, pghouse_name)

    print("Verifying row counts", file=sys.stderr)
    postgres_count = int(scalar(args, f"SELECT COUNT(*) FROM {postgres_name}"))
    pghouse_count = int(scalar(args, f"SELECT COUNT(*) FROM {pghouse_name}"))
    granule_count = int(
        scalar(
            args,
            f"SELECT COUNT(*) FROM pghouse.granules WHERE table_oid = '{pghouse_name}'::regclass",
        )
    )
    active_buffers = int(
        scalar(
            args,
            f"""
SELECT COUNT(*)
FROM pghouse.insert_buffers
WHERE table_oid = '{pghouse_name}'::regclass
  AND flushed_at IS NULL
""".strip(),
        )
    )

    postgres_agg_ms = measure_aggregates(args, "postgres", postgres_name)
    pghouse_agg_ms = measure_aggregates(args, "pghouse", pghouse_name)

    result = {
        "rows": args.rows,
        "batch_rows": args.batch_rows,
        "aggregate_repeats": args.agg_repeats,
        "postgres_table": postgres_name,
        "pghouse_table": pghouse_name,
        "postgres_insert_ms": postgres_insert_ms,
        "pghouse_insert_ms": pghouse_insert_ms,
        "postgres_row_count": postgres_count,
        "pghouse_row_count": pghouse_count,
        "pghouse_granule_count": granule_count,
        "pghouse_active_insert_buffers": active_buffers,
        "postgres_aggregate_ms": postgres_agg_ms,
        "pghouse_aggregate_ms": pghouse_agg_ms,
        "postgres_aggregate_avg_ms": round(sum(postgres_agg_ms) / len(postgres_agg_ms), 2),
        "pghouse_aggregate_avg_ms": round(sum(pghouse_agg_ms) / len(pghouse_agg_ms), 2),
        "notes": [
            "Current pghouse regular SQL aggregate queries still use the PostgreSQL table path.",
            "Insert timings include pghouse trigger and maintenance overhead for sidecar granule flush.",
        ],
    }

    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
