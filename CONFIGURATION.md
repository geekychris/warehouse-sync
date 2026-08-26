# Configuration reference

Every sync is one JSON file in `sync-configs/`. This document lists every field, when you'd use it, and how the choices interact.

Start with a working example in [sync-configs/users_sync.json](sync-configs/users_sync.json) or the [example gallery](#example-gallery) at the bottom of this doc. The JSON Schema at [config-schema/sync-config-schema.json](config-schema/sync-config-schema.json) is authoritative — run `warehouse-sync validate-config` after any edit.

## Top-level structure

```json
{
  "name":        "...",
  "sync_id":     "...",
  "version":     "1.0.0",
  "description": "...",

  "source":    { ... },
  "mode":      "append_only | merge | full_replace",
  "warehouse": { ... },
  "schedule":  { ... },
  "fields":    [ ... ]
}
```

## Top-level fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | ✓ | Human-readable label used in log messages. |
| `sync_id` | string, `^[a-z][a-z0-9_]*$` | ✓ | Stable identifier — used as the primary key in the state store. Rename = you lose history, so pick well. snake_case only. |
| `version` | string, SemVer | ✓ | Bump when the config changes in a way that changes what rows or columns land in the warehouse. Recorded on every row via `_sync_version`. |
| `description` | string | ✗ | Free-form. Show your reader why this sync exists. |

## `source` — where to read from

Common shape:

```json
"source": {
  "type": "postgres",
  "connection": {
    "host_env":     "SOURCE_PG_HOST",
    "port_env":     "SOURCE_PG_PORT",
    "database_env": "SOURCE_PG_DB",
    "user_env":     "SOURCE_PG_USER",
    "password_env": "SOURCE_PG_PASSWORD",
    "sslmode":      "prefer"
  },
  "schema": "public",
  "table":  "users",
  "columns":              ["id", "email", "display_name", "created_at", "updated_at"],
  "primary_key":          ["id"],
  "incremental_column":   "updated_at",
  "incremental_column_type": "timestamp"
}
```

| Field | Required for | Notes |
|---|---|---|
| `type` | all | Source discriminator. Resolved via ServiceLoader. Built-in: `postgres` (production), `mysql` (stub — pluggability proof), `debezium-kafka` (stub — CDC seam). Adding a new type = one class + one `META-INF/services` line. |
| `connection` | all | Free-form map — the source factory decides what keys mean. See [connection conventions](#connection-conventions). |
| `schema` | postgres | Source database schema. Optional but recommended so table names don't collide. |
| `table` | all | Source table name. |
| `columns` | ✗ | Whitelist of columns to sync. Missing/empty = all columns. Explicit whitelists insulate the warehouse from source-side column churn. |
| `primary_key` | `merge` | Ordered list of PK columns. Required by `merge`; ignored by other modes. |
| `incremental_column` | `merge`, `append_only` | Column the engine reads to decide "what's new" (`WHERE col > last_watermark ORDER BY col ASC LIMIT batch_size`). Typical: `updated_at`, `id`, `sequence_number`. |
| `incremental_column_type` | `merge`, `append_only` | Wire type of the column — drives SQL literal quoting and state-store comparisons. One of: `timestamp`, `long`, `int`, `date`, `string`. |

### Connection conventions

Prefer `<key>_env` suffix so secrets never live in the config file:

```json
"connection": {
  "host_env":     "SOURCE_PG_HOST",   // reads $SOURCE_PG_HOST
  "database_env": "SOURCE_PG_DB",
  "user_env":     "SOURCE_PG_USER",
  "password_env": "SOURCE_PG_PASSWORD"
}
```

The direct form (`host`, `database`, `user`, `password`) works too but is dangerous for anything with credentials. Ports and sslmode are usually fine inline.

## `mode` — how rows land

Three modes, chosen per table. The [decision matrix](ARCHITECTURE.md#write-modes-decision-matrix) in ARCHITECTURE.md walks through the tradeoffs; here's the summary:

| Mode | Source rows are... | Deletes | Watermark needed | Reads/writes |
|---|---|---|---|---|
| `append_only` | Immutable once written (event logs, order placements) | Not captured | ✓ | Cheapest — straight append |
| `merge` | Edited in place, PK stable | Not captured — pair with `full_refresh_cadence` if needed | ✓ | Iceberg `MERGE INTO` on PK — upsert |
| `full_replace` | Small reference table or churn-heavy | Caught by whole-table snapshot | ✗ | Most expensive — reads + writes whole table each run |

Modes are enforced structurally by [`SyncConfig.validateStructural`](src/main/java/com/warehouse/sync/config/SyncConfig.java):

- `merge` requires `primary_key` **and** `incremental_column`
- `append_only` requires `incremental_column`
- `full_replace` requires neither

`validate-config` surfaces these before you ever start Spark.

## `warehouse` — where rows land

```json
"warehouse": {
  "table_name":     "warehouse.public_users",
  "target_catalog": "iceberg",
  "partition_by":   ["updated_date"],
  "sort_by":        ["id"]
}
```

| Field | Required | Notes |
|---|---|---|
| `table_name` | ✓ | Fully qualified `<namespace>.<table>`. The namespace is auto-created if missing. |
| `target_catalog` | ✗ (default `iceberg`) | Spark catalog name (`spark.sql.catalog.<name>`). Matches whatever `SparkSessionFactory` registered. |
| `partition_by` | ✗ | Iceberg partition columns. Applied at `CREATE TABLE` time; changing later requires a migration. |
| `sort_by` | ✗ | Iceberg sort order. Aims at query planners; does not enforce ordering on write. |

## `schedule` — when and how much

```json
"schedule": {
  "cadence":              "5m",
  "batch_size":           10000,
  "full_refresh_cadence": "24h",
  "watermark_lag_seconds": 5
}
```

| Field | Required | Notes |
|---|---|---|
| `cadence` | ✓ | Duration string (`30s`, `5m`, `2h`, `1d`). Consumed by `warehouse-sync serve`; one-shot `run` ignores it. Cron4j at minute granularity or an internal ScheduledExecutorService for sub-minute cadences. |
| `batch_size` | ✓ | Max rows per `LIMIT` clause. Engine loops until fewer than `batch_size` rows come back (drained). Larger batches use more memory but fewer round-trips. |
| `full_refresh_cadence` | ✗ | Even on `merge` or `append_only`, periodically run a full-replace snapshot to catch deletes and drift. `24h` is a reasonable default for tables where deletes matter. |
| `watermark_lag_seconds` | ✗ | For `timestamp` incremental columns only: don't read rows younger than N seconds. Guards against races with in-flight source-side transactions whose commits land between two `max()` reads. Set to 5–30s for high-write sources. |

## `fields` — column overrides (optional)

Rare — most configs skip this entirely. Use it when you want to:

- **Rename a column** in the warehouse without touching the source:
  ```json
  "fields": [
    { "name": "email", "warehouse_name": "email_addr" }
  ]
  ```
- **Override the auto-detected Iceberg type**:
  ```json
  "fields": [
    { "name": "amount", "type": "DECIMAL(18, 2)" }
  ]
  ```

Missing `fields` = source column names and Spark-inferred types are used verbatim.

## Envelope columns (added automatically)

Every synced row gets six metadata columns appended by [`SyncEngine.enrich`](src/main/java/com/warehouse/sync/engine/SyncEngine.java) before the writer sees it. You don't declare these — they always exist alongside your business columns:

| Column | Type | Purpose |
|---|---|---|
| `_sync_id` | STRING | Which config produced this row |
| `_sync_version` | STRING | Config `version` at time of write — useful when a version bump changes column meanings |
| `_ingested_at` | TIMESTAMP | Wall-clock at batch start |
| `_ingested_date` | DATE | UTC date, natural partition key |
| `_source_op` | STRING | `UPSERT` (append_only/merge) or `SNAPSHOT` (full_replace) |
| `_batch_id` | STRING | UUID per SyncEngine batch; groups every row written in one `write()` call |

Query examples:

```sql
-- Rows that came from a specific run
SELECT * FROM warehouse.public_users WHERE _batch_id = '0b48b100-...';

-- Rows synced today
SELECT COUNT(*) FROM warehouse.public_users WHERE _ingested_date = current_date;

-- Latest version's rows only
SELECT * FROM warehouse.public_users WHERE _sync_version = '1.1.0';
```

## Runtime environment variables

The Spark session picks these up via [`SparkSessionFactory`](src/main/java/com/warehouse/sync/spark/SparkSessionFactory.java). All optional, all defaulted for the local lakehouse stack:

| Variable | Default | Meaning |
|---|---|---|
| `WAREHOUSE_SYNC_CATALOG` | `iceberg` | Spark catalog name — match your `warehouse.target_catalog` |
| `WAREHOUSE_SYNC_WAREHOUSE_PATH` | `s3a://warehouse` | Iceberg warehouse root |
| `WAREHOUSE_SYNC_METASTORE_URI` | `thrift://localhost:18083` | Hive metastore Thrift URI |
| `WAREHOUSE_SYNC_S3_ENDPOINT` | `http://localhost:18000` | S3-compatible endpoint (MinIO in local dev) |
| `WAREHOUSE_SYNC_S3_ACCESS_KEY` | `admin` | |
| `WAREHOUSE_SYNC_S3_SECRET_KEY` | `password123` | |
| `WAREHOUSE_SYNC_SPARK_MASTER` | `local[*]` | Set to `spark://…` to run against a cluster |
| `WAREHOUSE_SYNC_LOG_LEVEL` | `WARN` | Spark log level |
| `WAREHOUSE_SYNC_JAVA_OPTS` | — | Extra JVM args appended by the launcher (heap, GC tuning, etc.) |

Source-connection env vars are whatever your configs' `*_env` fields reference. For the shipped demo configs:

```bash
export SOURCE_PG_HOST=127.0.0.1
export SOURCE_PG_PORT=15433
export SOURCE_PG_DB=source
export SOURCE_PG_USER=sync
export SOURCE_PG_PASSWORD=sync
```

## Validation

Before every commit, run:

```bash
./bin/warehouse-sync validate-config --config sync-configs/
```

Checks in order:

1. **JSON Schema** — types, enums, required fields, regex patterns (`sync_id` snake_case, `cadence` duration format, etc.)
2. **Structural** — mode-specific rules (`merge` needs PK, etc.)
3. **Source factory preflight** — the resolved `SourceFactory.validate()` runs its own checks (connection has the keys the source needs, etc.) — no network I/O.

Any failure blocks commit if wired to a pre-commit hook. `validate-config` exits `2` for JSON/structural errors and `1` if a source factory rejects the config.

## Example gallery

### Merge (edits in place, catches deletes nightly)

```json
{
  "name": "UsersSync",
  "sync_id": "users_sync",
  "version": "1.0.0",
  "source": {
    "type": "postgres",
    "connection": { "host_env": "PG_HOST", "database_env": "PG_DB", "user_env": "PG_USER", "password_env": "PG_PASS" },
    "schema": "public", "table": "users",
    "columns": ["id", "email", "display_name", "created_at", "updated_at"],
    "primary_key": ["id"],
    "incremental_column": "updated_at",
    "incremental_column_type": "timestamp"
  },
  "mode": "merge",
  "warehouse": { "table_name": "warehouse.public_users", "sort_by": ["id"] },
  "schedule": {
    "cadence": "5m",
    "batch_size": 10000,
    "full_refresh_cadence": "24h",
    "watermark_lag_seconds": 5
  }
}
```

### Append-only (immutable events)

```json
{
  "name": "OrdersSync",
  "sync_id": "orders_sync",
  "version": "1.0.0",
  "source": {
    "type": "postgres",
    "connection": { "host_env": "PG_HOST", "database_env": "PG_DB", "user_env": "PG_USER", "password_env": "PG_PASS" },
    "schema": "public", "table": "orders",
    "columns": ["id", "user_id", "amount_cents", "status", "created_at"],
    "primary_key": ["id"],
    "incremental_column": "id",
    "incremental_column_type": "long"
  },
  "mode": "append_only",
  "warehouse": { "table_name": "warehouse.public_orders", "sort_by": ["created_at"] },
  "schedule": { "cadence": "1m", "batch_size": 5000 }
}
```

### Full replace (small reference table, deletes matter)

```json
{
  "name": "ProductsFullRefresh",
  "sync_id": "products_full_refresh",
  "version": "1.0.0",
  "source": {
    "type": "postgres",
    "connection": { "host_env": "PG_HOST", "database_env": "PG_DB", "user_env": "PG_USER", "password_env": "PG_PASS" },
    "schema": "public", "table": "products",
    "columns": ["id", "sku", "name", "price_cents", "active"],
    "primary_key": ["id"]
  },
  "mode": "full_replace",
  "warehouse": { "table_name": "warehouse.public_products" },
  "schedule": { "cadence": "1h", "batch_size": 100000 }
}
```

### Watermark on a monotonic sequence (Postgres BIGSERIAL)

```json
"source": {
  "type": "postgres",
  ...
  "incremental_column": "id",
  "incremental_column_type": "long"
}
```

Faster than `updated_at`-based watermarks — no timestamp comparisons, no timezone concerns, works with unindexed `updated_at`.

### Rename columns between source and warehouse

```json
"fields": [
  { "name": "email",       "warehouse_name": "email_addr" },
  { "name": "display_name", "warehouse_name": "full_name"  }
]
```

Rest of the config unchanged. Handy when the warehouse follows a different naming convention.

## Common gotchas

- **`sync_id` collisions.** Two configs with the same `sync_id` will fight over one row in `sync_state.watermarks` and produce a chaotic run history. `validate-config` doesn't cross-check configs against each other — you have to keep IDs unique yourself.
- **Watermark column that isn't monotonic.** If `updated_at` can go backward (e.g. a nightly bulk-import script sets it to a fixed value), the engine will miss rows. Use `id` on immutable tables or add a `watermark_lag_seconds` guard.
- **`merge` mode without a PK unique in the source.** Iceberg `MERGE INTO` produces duplicate updates on multi-match. Add a unique constraint upstream or switch to `full_replace`.
- **Column added to source after first sync.** Iceberg auto-evolves the target schema if the writer uses `.append()` / `.overwrite()` on a Dataset whose schema is a superset — but only for compatible additions. Removed or retyped columns need explicit migration.
- **Batch size too small for large tables.** Engine loops until drained, so tiny batches on a big backfill = many round-trips. Set `batch_size` to 10k–100k for the first-ever run; can be smaller on the incremental schedule.
