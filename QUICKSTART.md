# warehouse-sync quickstart

Zero → first row in the warehouse in five minutes.

## Prereqs

- Docker (Rancher Desktop, Docker Desktop, or Colima)
- Java 11+ and Maven 3.9+
- A checkout of `spark_minio_trino` — either at `../spark_minio_trino` or export `SPARK_MINIO_TRINO_DIR=/path/to/it`

## 1. Bring up the stack

```bash
./scripts/start-stack.sh
```

This brings up MinIO + Trino + Hive metastore + Spark + a demo Postgres seeded with `users`, `orders`, and `products` tables (see `scripts/init-source-postgres.sql`).

Endpoints:

- MinIO console: <http://localhost:18001>  (`admin` / `password123`)
- Trino: <http://localhost:18013>
- Source Postgres: `postgres://sync:sync@localhost:15433/source`

## 2. Build

```bash
mvn -DskipTests package
```

Produces `target/warehouse-sync-0.1.0-all.jar` (~500MB fat jar).

## 3. Validate the sample configs

```bash
java -jar target/warehouse-sync-0.1.0-all.jar validate-config --config sync-configs/
```

## 4. Dry-run one sync

```bash
export SOURCE_PG_HOST=127.0.0.1 SOURCE_PG_PORT=15433 SOURCE_PG_DB=source SOURCE_PG_USER=sync SOURCE_PG_PASSWORD=sync
java -jar target/warehouse-sync-0.1.0-all.jar dry-run --config sync-configs/users_sync.json
```

You'll see the SELECT that would run, the row count, sample rows, and the watermark that WOULD be advanced to. Nothing is written.

## 5. Run for real

```bash
java -jar target/warehouse-sync-0.1.0-all.jar run --config sync-configs/
```

Every config in `sync-configs/` runs once and exits. Watch for `status=OK` lines.

## 6. Check state

```bash
java -jar target/warehouse-sync-0.1.0-all.jar inspect
```

Prints watermarks + recent run history per sync.

## 7. Query the warehouse

```bash
./scripts/query-warehouse.sh iceberg "SELECT * FROM iceberg.warehouse.public_users LIMIT 5"
./scripts/query-warehouse.sh iceberg "SELECT COUNT(*) FROM iceberg.warehouse.public_orders"
./scripts/query-warehouse.sh iceberg "SELECT * FROM iceberg.sync_state.run_history ORDER BY started_at DESC LIMIT 10"
```

## 8. Watch incremental in action

```bash
./scripts/seed-postgres.sh        # inserts + updates + deactivations
java -jar target/warehouse-sync-0.1.0-all.jar run --config sync-configs/
java -jar target/warehouse-sync-0.1.0-all.jar inspect
```

`inspect` will show the watermark advanced and rows_synced > 0. Query `warehouse.public_users` again to see the merges landed.

## 9. Run as a daemon

```bash
java -jar target/warehouse-sync-0.1.0-all.jar serve --config-dir sync-configs/
```

Uses each config's `schedule.cadence`. Ctrl-C for graceful shutdown.

## One-liner

```bash
./demo/demo-end-to-end.sh
```

Runs steps 1 through 8 in sequence.

## Troubleshooting

- **`ERROR StatusLogger Unrecognized format specifier`** — cosmetic. Spark bundles log4j 1.x alongside our log4j 2 config. Ignore.
- **Testcontainers `client version 1.32 is too old`** — you're on Rancher Desktop. `export DOCKER_HOST=unix:///Users/$USER/.rd/docker.sock` and re-run.
- **`spark_minio_trino` not found** — set `export SPARK_MINIO_TRINO_DIR=/path/to/spark_minio_trino`.
- **Trino says table doesn't exist** — check that `warehouse-sync run` actually returned `status=OK`; a `NOOP` means no source rows since the last watermark.
