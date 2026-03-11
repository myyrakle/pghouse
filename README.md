# pghouse

`pghouse` is an in-progress PostgreSQL extension that starts from a custom table access
method and layers a sidecar columnar/granule store on top of it.

Current bootstrap behavior:

- `CREATE TABLE ... USING pghouse` works because the access method delegates to `heap` for now.
- `pghouse_register_table(...)` installs DML capture triggers and registers merge metadata.
- INSERT/UPDATE/DELETE are copied into an async queue.
- `pghouse_run_maintenance_once()` or `pghouse_launch_worker()` folds queued mutations into
  a PK-oriented snapshot and stores compressed column chunks in `pghouse.column_chunks`.

This is not the final storage engine yet. The next steps are replacing the heap delegate with a
real `TableAmRoutine`, WAL-aware granule files/pages, and planner/executor integration for reads.
