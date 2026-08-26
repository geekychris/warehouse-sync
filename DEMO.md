# Demo walkthrough

Two demos ship with the project:

- **`demo/playground.sh`** — interactive, five-scenario tour with pauses between steps. Great for a live walkthrough.
- **`demo/demo-end-to-end.sh`** — non-interactive, runs the full sequence in one shot. Great for CI or a smoke test.

Both use the same seeded lakehouse + source Postgres. Prereqs: [QUICKSTART.md](QUICKSTART.md) covers first-time setup (stack up, jar built, env vars).

## Quick check before you start

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E "(source-postgres|hive|trino|spark|minio)"
```

You should see `source-postgres`, `hive-metastore`, `trino`, `spark-master`, `minio` — all healthy. If not:

```bash
./scripts/start-stack.sh
```

And the jar exists:

```bash
ls -lh target/warehouse-sync-*-all.jar || mvn -DskipTests package
```

## The interactive playground

```bash
./demo/playground.sh
```

The script guides you through five scenarios, pausing between each so you can read the output. Press Enter to advance, Ctrl-C to bail. The env vars are set inside the script — nothing to export.

### Scenario 1: append_only mode

**What it shows:** Immutable events streamed into the warehouse with a monotonic `id` watermark.

**What happens:**

1. Prints current source + warehouse row counts (should match after a fresh sync).
2. Inserts 5 fresh orders into source Postgres:
   ```sql
   INSERT INTO orders (user_id, amount_cents, status)
   SELECT (SELECT id FROM users ORDER BY random() LIMIT 1),
          (500 + floor(random()*5000))::bigint, 'placed'
   FROM generate_series(1,5);
   ```
3. Counts again — source is 5 ahead of the warehouse.
4. You hit Enter, it runs `warehouse-sync run --config sync-configs/orders_sync.json`.
5. Counts again — warehouse caught up.
6. Runs `warehouse-sync inspect --sync-id orders_sync` to show the state store record: watermark advanced from `N` to `N+5`, run status OK.

**What to look for:** The watermark in `inspect` output. Before the sync it's the last order id at rest; after, it's the new max.

### Scenario 2: merge mode

**What it shows:** Upsert-on-PK. Editing a row in source updates the SAME row in the warehouse — no duplicates.

**What happens:**

1. Renames user id=1 in source:
   ```sql
   UPDATE users SET display_name = 'Alice (edited at ...)' WHERE id = 1;
   ```
2. Prints the source row and the current warehouse row side-by-side. Warehouse is stale.
3. You hit Enter → `warehouse-sync run --config sync-configs/users_sync.json`.
4. Prints the warehouse row again — updated in place, same PK, no dupe.

**What to look for:** `SELECT COUNT(*) FROM warehouse.public_users` doesn't change after the merge. The `MERGE INTO` writer matches on the primary key and updates rather than inserts.

**Bonus:** The Postgres source table has a `BEFORE UPDATE` trigger that bumps `updated_at` on every edit. That's what advances the merge watermark.

### Scenario 3: full_replace mode

**What it shows:** Whole-table snapshots. Deletes and deactivations show up because the target is replaced atomically.

**What happens:**

1. Prints the current warehouse products table.
2. Deactivates SKU-1 in source: `UPDATE products SET active = false WHERE sku = 'SKU-1';`
3. Prints source products (SKU-1 now `active=false`).
4. You hit Enter → `warehouse-sync run --config sync-configs/products_full_refresh.json`.
5. Prints the warehouse — now matches source, `active=false` propagated.

**What to look for:** `full_replace` doesn't need `incremental_column` or `primary_key` — see how the config file is simpler. Also: it always writes the whole table, so it's expensive for big tables — designed for small reference data.

### Scenario 4: dry-run

**What it shows:** Preview without side effects. Great for validating a new config before you unleash it.

**What happens:**

1. Inserts 3 new users in source — but doesn't sync yet.
2. Prints source + warehouse counts — source is 3 ahead.
3. You hit Enter → `warehouse-sync dry-run --config sync-configs/users_sync.json`.
4. Output shows:
   - Current watermark (from state store)
   - Row count that WOULD be synced (3)
   - Sample rows
   - Watermark that WOULD be advanced to
5. Prints source + warehouse counts again — **still** 3 apart. Dry-run advanced nothing.

**What to look for:** No `_batch_id` in the warehouse for these 3 users after dry-run — they're still only in Postgres. Run a real sync to move them.

### Scenario 5: Iceberg time travel + audit log

**What it shows:** Iceberg keeps every commit, and warehouse-sync keeps every run.

**What happens:**

1. Queries `warehouse.public_users$snapshots` — Iceberg's built-in metadata table with `committed_at`, `snapshot_id`, `rows_added`, `rows_deleted` for every commit made to this table.
2. Queries `sync_state.run_history` — warehouse-sync's own audit log. One row per batch, with status, timing, watermark before/after.

**What to look for:** The correspondence between snapshot commits and sync runs. Every OK/rows>0 run in `run_history` should have a matching snapshot in `public_users$snapshots`. NOOP runs don't produce snapshots because no data changed.

**Bonus queries to try after the demo:**

```sql
-- Point-in-time query: warehouse state as of a specific snapshot
SELECT * FROM warehouse.public_users
FOR VERSION AS OF <snapshot_id>;

-- Failed runs and their errors
SELECT sync_id, started_at, error_message
FROM iceberg.sync_state.run_history
WHERE status = 'FAILED';

-- Rows synced per config over time
SELECT sync_id, DATE(started_at) AS d, SUM(rows_synced) AS total
FROM iceberg.sync_state.run_history
GROUP BY sync_id, DATE(started_at)
ORDER BY d DESC;
```

## The one-shot demo

```bash
./demo/demo-end-to-end.sh
```

No pauses. Runs steps 1 through 8 straight through: bring up stack, build jar, seed, validate, dry-run all configs, run all configs, seed churn again, run again, inspect, query Trino. Suitable for CI or a fresh-clone smoke test.

Expected output ends with:

```
✓ demo complete
```

If any step fails it exits non-zero. Common failure modes:

- Stack didn't come up cleanly — check `docker ps`, re-run `./scripts/start-stack.sh`
- Old volume left over from a prior run — nuke with `./scripts/reset-and-rebuild.sh`
- `spark_minio_trino` not found — set `SPARK_MINIO_TRINO_DIR` or symlink

## Extending the demo yourself

The playground is a plain bash script — [`demo/playground.sh`](demo/playground.sh). It reads short and is easy to fork. Common extensions:

### Add a scenario that fails on purpose

Deactivate the source before running to see what a FAILED status looks like:

```bash
docker stop source-postgres
./bin/warehouse-sync run --config sync-configs/users_sync.json    # → FAILED
docker start source-postgres
./bin/warehouse-sync inspect --sync-id users_sync                  # see the error row
```

### Watch the daemon react to inserts

In terminal 1:

```bash
./bin/warehouse-sync serve --config-dir sync-configs/
```

In terminal 2:

```bash
# Insert a new order every 30 seconds and watch orders_sync (cadence=1m) pick it up.
while true; do
  docker exec source-postgres psql -U sync -d source -c "INSERT INTO orders (user_id, amount_cents, status) VALUES (1, 999, 'placed');"
  sleep 30
done
```

Terminal 1 will log each scheduled tick pulling the new rows.

### Test a schema evolution

Add a column to the source, re-sync, verify:

```bash
docker exec source-postgres psql -U sync -d source -c "ALTER TABLE users ADD COLUMN phone TEXT;"
docker exec source-postgres psql -U sync -d source -c "UPDATE users SET phone = '+1-555-0100' WHERE id = 1;"

# Update the config to include the new column
# (edit sync-configs/users_sync.json — add "phone" to source.columns)

./bin/warehouse-sync run --config sync-configs/users_sync.json
docker exec trino trino --catalog iceberg --execute "SELECT id, email, phone FROM warehouse.public_users WHERE id=1"
```

### Backfill after a bug

Suppose you notice the last week's data in the warehouse is wrong:

```bash
# Reset the watermark to a week ago
./bin/warehouse-sync backfill --config sync-configs/users_sync.json \
    --watermark '2026-08-18T00:00:00Z'

# Or nuke the watermark entirely and re-pull the whole source table:
./bin/warehouse-sync backfill --config sync-configs/users_sync.json --reset
```

For `merge` mode, the re-pull is idempotent — every incoming row upserts on PK. For `append_only`, backfill will duplicate rows unless you also truncate the target first.

## Cleaning up

Between playground runs the source state accumulates (each `INSERT` from the playground stays). To reset:

```bash
# Fastest — wipe the source-postgres volume and re-init from scripts/init-source-postgres.sql
docker compose -f /Users/chris/code/warp_experiments/done/spark_minio_trino/docker-compose.yml \
               -f docker-compose.yml \
               -f lakehouse-override.yml \
               down -v

./scripts/start-stack.sh
```

Or, to keep source data but wipe the warehouse:

```bash
# Drop just the warehouse tables and let the next sync re-create them
docker exec trino trino --catalog iceberg --execute "DROP TABLE warehouse.public_users; DROP TABLE warehouse.public_orders; DROP TABLE warehouse.public_products; DROP TABLE sync_state.watermarks; DROP TABLE sync_state.run_history"
```

Then re-run `warehouse-sync run --config sync-configs/` for a clean re-sync.
