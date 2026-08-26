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

- **`ERROR StatusLogger Unrecognized format specifier`** — cosmetic. Spark bundles a log4j 1.x pattern that our log4j 2 setup can't parse. Ignore.
- **Testcontainers `client version 1.32 is too old`** (Rancher Desktop) — add `api.version=1.43` to `~/.docker-java.properties` and `docker.host=unix:///Users/$USER/.rd/docker.sock` + `ryuk.disabled=true` to `~/.testcontainers.properties`, then `sudo ln -s /Users/$USER/.rd/docker.sock /var/run/docker.sock` so testcontainers' Unix strategy finds the socket.
- **`spark_minio_trino` not found** — set `export SPARK_MINIO_TRINO_DIR=/path/to/spark_minio_trino` or symlink it as `../spark_minio_trino`.
- **Port collisions on stack-up** (e.g. `Bind for 127.0.0.1:15432 failed`) — an ssh tunnel or another docker stack owns the port. Edit `lakehouse-override.yml` to move the offending port (the `!override` tag replaces the base compose's mapping cleanly).
- **`Namespace does not exist: warehouse`** — the engine now `CREATE NAMESPACE IF NOT EXISTS` before the first table, so this should only appear if you overrode the target catalog to one that doesn't have the namespace and can't create it.
- **`invalid value for parameter "TimeZone": "US/Pacific"`** — Postgres JDBC ships the JVM's default timezone to the server. `bin/warehouse-sync` pins `-Duser.timezone=UTC`. If you run `java -jar` directly, add the flag yourself.
- **Trino says table doesn't exist** — check `warehouse-sync inspect`; a `NOOP` means no source rows since the last watermark.
