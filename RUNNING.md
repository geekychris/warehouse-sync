# Running warehouse-sync

Every command works in three environments — local dev, cluster, CI — with the same jar and same configs. This doc covers each mode and every subcommand.

## Prereqs

| | Version | Notes |
|---|---|---|
| Docker | any recent | Rancher Desktop / Docker Desktop / Colima. Local dev only. |
| JDK | 11+ (21 tested) | Launcher script (`bin/warehouse-sync`) adds `--add-opens` for JDK 17+ |
| Maven | 3.9+ | Build only |
| Trino CLI | optional | `brew install trinodb/tap/trino-cli`. Not required — `docker exec trino trino …` works. |
| `spark_minio_trino` | any recent | Local dev only. Sibling directory or `export SPARK_MINIO_TRINO_DIR=…` |

## Build

```bash
mvn -DskipTests package     # builds target/warehouse-sync-0.1.0-all.jar (~530MB fat jar)
```

Or via Makefile:

```bash
make build
```

Unit tests (16, sub-second):

```bash
mvn test
```

Integration tests (needs docker):

```bash
mvn verify
```

## Invocation styles

### The launcher (recommended)

`bin/warehouse-sync` wraps `java -jar` with the JVM flags Spark needs (fourteen `--add-opens` for JDK 17+, `-Duser.timezone=UTC` for the Postgres driver):

```bash
./bin/warehouse-sync <subcommand> [args...]
```

### Direct `java -jar`

If you're not on JDK 21 or don't need the launcher's defaults, you can invoke the jar directly:

```bash
java -jar target/warehouse-sync-*-all.jar <subcommand> [args...]
```

You'll want to add `-Duser.timezone=UTC` if you're syncing Postgres (else you'll hit `FATAL: invalid value for parameter "TimeZone"`), and the `--add-opens` flags on JDK 17+ (else Spark won't init).

### Extra JVM tuning

```bash
export WAREHOUSE_SYNC_JAVA_OPTS="-Xmx4g -XX:+UseG1GC"
./bin/warehouse-sync run --config sync-configs/
```

The launcher appends `$WAREHOUSE_SYNC_JAVA_OPTS` after the required flags.

## Environment

The engine reads two sets of env vars:

**Warehouse connection** — where the Iceberg tables live. All optional, all defaulted for the local lakehouse. See [CONFIGURATION.md](CONFIGURATION.md#runtime-environment-variables) for the full table.

**Source connection** — set by whatever `*_env` fields your configs reference. For the shipped demo configs:

```bash
export SOURCE_PG_HOST=127.0.0.1
export SOURCE_PG_PORT=15433
export SOURCE_PG_DB=source
export SOURCE_PG_USER=sync
export SOURCE_PG_PASSWORD=sync
```

For CI or prod, prefer a `.env` file or secret manager. The engine doesn't ship one — it deliberately delegates secret handling to the environment.

## Subcommands

### `validate-config`

```bash
./bin/warehouse-sync validate-config --config sync-configs/
./bin/warehouse-sync validate-config --config sync-configs/users_sync.json
```

Runs three checks:

1. JSON Schema (types, enums, regex patterns)
2. Structural rules (`merge` needs PK, etc.)
3. Source factory preflight (connection has required keys)

**No network I/O.** Safe to run anywhere; ideal for pre-commit hooks and CI.

Exit codes: `0` valid, `1` source factory rejects, `2` schema/structural failure.

### `dry-run`

```bash
./bin/warehouse-sync dry-run --config sync-configs/users_sync.json
./bin/warehouse-sync dry-run --config sync-configs/ --sample 20
```

For each config: reads the source, prints:

- Current watermark from the state store
- Row count that WOULD be synced
- Up to `--sample` (default 5) sample rows
- Watermark that WOULD be advanced to

**Does NOT write to the warehouse. Does NOT advance the state store.** Use before running a new config to sanity-check the SELECT.

Requires everything a real run does (Spark, Iceberg, source connection) — it stops short only of the writer + state advance.

### `run`

```bash
./bin/warehouse-sync run --config sync-configs/users_sync.json      # one config
./bin/warehouse-sync run --config sync-configs/                     # every config in dir
./bin/warehouse-sync run --config sync-configs/ --force-full-refresh
```

One-shot: loads configs, runs each end-to-end, exits. Loops per config until source is drained (returned batch < `batch_size`).

`--force-full-refresh` overrides the config's declared mode for this run, snapshotting the whole source table regardless. Useful for reconciliation after a suspected drift.

Prints one line per config:

```
[users_sync] status=OK rows=6 batches=1 watermark=Watermark[TIMESTAMP=...]→Watermark[TIMESTAMP=...] took=PT7.4S
```

Statuses: `OK` (rows synced), `NOOP` (source had nothing new), `FAILED` (see `inspect` for the error).

Exit code: 0 if every config was OK or NOOP; 1 if any FAILED.

Suitable for cron / systemd-timer / k8s CronJob:

```bash
# /etc/cron.d/warehouse-sync
*/5 * * * *  chris  /path/to/warehouse-sync run --config /etc/warehouse-sync/
```

### `serve`

```bash
./bin/warehouse-sync serve --config-dir sync-configs/
```

Long-running daemon. Reads every config in the directory, schedules each on its declared `schedule.cadence`. Ctrl-C for graceful shutdown (waits for in-flight batches up to 30s per sync).

Schedulers:

- **≥ 1 minute cadence** — cron4j, 5-field cron pattern derived from the duration.
- **< 1 minute cadence** — internal `ScheduledExecutorService` with fixed-delay.

Concurrency: each `sync_id` has a `ReentrantLock`. A second scheduled tick that finds a run still in flight is skipped and logged (`previous run still in flight — skipping this tick`). Next tick will find fresh rows and drain.

Periodic full refresh: if a config declares `schedule.full_refresh_cadence`, the daemon runs a `full_replace` sync (regardless of the declared mode) that often. Catches deletes and drift in `merge`/`append_only` tables.

Best deployed under systemd or supervisord so it auto-restarts:

```ini
# /etc/systemd/system/warehouse-sync.service
[Unit]
Description=warehouse-sync daemon
After=network.target

[Service]
User=warehouse-sync
EnvironmentFile=/etc/warehouse-sync/env
ExecStart=/opt/warehouse-sync/bin/warehouse-sync serve --config-dir /etc/warehouse-sync/configs/
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### `inspect`

```bash
./bin/warehouse-sync inspect                              # every sync
./bin/warehouse-sync inspect --sync-id users_sync         # one
./bin/warehouse-sync inspect --limit 20                   # more history rows
```

Reads the Iceberg-backed state store and prints:

- Current watermark + type + when it was last advanced
- Recent runs (default 5), each with status, row count, watermark before/after, error message if any

Read-only — safe to run anytime, does not disturb in-flight syncs.

For richer queries hit the state store directly via Trino:

```sql
SELECT * FROM iceberg.sync_state.run_history WHERE status = 'FAILED' ORDER BY started_at DESC;
SELECT sync_id, COUNT(*), SUM(rows_synced), AVG(EXTRACT(SECOND FROM (finished_at - started_at))) AS avg_s
  FROM iceberg.sync_state.run_history
 WHERE started_at > NOW() - INTERVAL '7' DAY
 GROUP BY sync_id;
```

### `backfill`

```bash
./bin/warehouse-sync backfill --config sync-configs/users_sync.json --reset
./bin/warehouse-sync backfill --config sync-configs/users_sync.json --watermark '2026-08-01T00:00:00Z'
./bin/warehouse-sync backfill --config sync-configs/users_sync.json --watermark '1000' --no-run
```

Move or reset a watermark, then re-sync. Provide exactly one of `--reset` or `--watermark <value>`.

- `--reset` — set watermark to UNSET; next run re-pulls the entire source table
- `--watermark <value>` — set to a specific literal (formatted per `incremental_column_type`)
- `--no-run` — change state but don't trigger a sync

For `merge` mode, backfilling is idempotent (every row upserts on PK). For `append_only`, backfilling will **duplicate rows** in the warehouse unless you also truncate the target first — this is the tradeoff of the mode.

## Common workflows

### First-time setup (local dev)

```bash
./scripts/start-stack.sh
mvn -DskipTests package
export SOURCE_PG_HOST=127.0.0.1 SOURCE_PG_PORT=15433 SOURCE_PG_DB=source \
       SOURCE_PG_USER=sync SOURCE_PG_PASSWORD=sync
./bin/warehouse-sync validate-config --config sync-configs/
./bin/warehouse-sync run --config sync-configs/
./bin/warehouse-sync inspect
```

### Add a new sync config

```bash
cp sync-configs/users_sync.json sync-configs/my_new_sync.json
# edit — see CONFIGURATION.md for every field

./bin/warehouse-sync validate-config --config sync-configs/my_new_sync.json
./bin/warehouse-sync dry-run          --config sync-configs/my_new_sync.json
./bin/warehouse-sync run              --config sync-configs/my_new_sync.json
./bin/warehouse-sync inspect --sync-id my_new_sync
```

### Debug a failing sync

```bash
./bin/warehouse-sync inspect --sync-id troubled_sync --limit 20
# See status=FAILED entries + their error_message

# Reproduce with dry-run — safer than a real run:
./bin/warehouse-sync dry-run --config sync-configs/troubled_sync.json

# Or query the state store directly for structured debugging:
docker exec -it trino trino --catalog iceberg --execute \
  "SELECT run_id, started_at, error_message FROM sync_state.run_history
    WHERE sync_id='troubled_sync' AND status='FAILED' ORDER BY started_at DESC LIMIT 10"
```

### Bump a config's version

Change the version string in the config, redeploy the config file, next run's rows will carry the new `_sync_version`. Old rows keep their old version, so you can filter or migrate as needed:

```sql
-- Rows written by the old version
SELECT * FROM warehouse.public_users WHERE _sync_version = '1.0.0';

-- Migrate them (example: fill in a new column that older versions didn't provide)
UPDATE warehouse.public_users SET new_col = 'default' WHERE _sync_version = '1.0.0';
```

### Reset everything and start clean

```bash
# Nuke docker volumes → wipes source Postgres + Hive metastore state + MinIO storage
docker compose -f /path/to/spark_minio_trino/docker-compose.yml \
               -f docker-compose.yml \
               -f lakehouse-override.yml \
               down -v

./scripts/reset-and-rebuild.sh   # ↑ + rebuild jar + restart stack
```

## Ports & endpoints (local dev)

| Service | Host port | URL / connection |
|---|---|---|
| MinIO console | 18001 | http://localhost:18001 (admin/password123) |
| Trino | 18013 | http://localhost:18013 |
| Spark master UI | 18082 | http://localhost:18082 |
| Source Postgres | 15433 | `postgres://sync:sync@localhost:15433/source` |
| Metastore Postgres | 15434 | (internal — used by Hive metastore) |
| Hive Metastore | 18083 | `thrift://localhost:18083` |

## Deploying to a real Spark cluster

The fat jar is self-contained but hefty (~530MB). For a cluster:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.warehouse.sync.cli.Main \
    --conf spark.driver.userClassPathFirst=true \
    target/warehouse-sync-0.1.0-all.jar \
    run --config /path/to/sync-configs/
```

Set `WAREHOUSE_SYNC_SPARK_MASTER` if you want the built-in launcher path instead of spark-submit — the engine builds its own `SparkSession` and can attach to a remote master.

For a k8s CronJob, package the jar in a container image with `bin/warehouse-sync` and mount configs from a ConfigMap.
