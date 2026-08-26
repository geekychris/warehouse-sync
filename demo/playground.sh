#!/usr/bin/env bash
# Interactive playground. Walks you through five scenarios one at a time
# so you can SEE each one land in the warehouse.
#
# Usage:  ./demo/playground.sh
# Ctrl-C at any prompt to exit.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

export SOURCE_PG_HOST=127.0.0.1
export SOURCE_PG_PORT=15433
export SOURCE_PG_DB=source
export SOURCE_PG_USER=sync
export SOURCE_PG_PASSWORD=sync
export WAREHOUSE_SYNC_METASTORE_URI=thrift://localhost:18083
export WAREHOUSE_SYNC_S3_ENDPOINT=http://localhost:18000
export WAREHOUSE_SYNC_S3_ACCESS_KEY=admin
export WAREHOUSE_SYNC_S3_SECRET_KEY=password123
export WAREHOUSE_SYNC_WAREHOUSE_PATH=s3a://warehouse

WS=./bin/warehouse-sync

# Bail early with a friendly message if the stack isn't up.
if ! docker ps --format '{{.Names}}' | grep -q '^source-postgres$'; then
    echo "source-postgres container not running — run ./scripts/start-stack.sh first." >&2
    exit 1
fi

pg()    { docker exec -i source-postgres psql -U sync -d source -v ON_ERROR_STOP=1 "$@"; }
trino() { docker exec -i trino trino --catalog iceberg --execute "$1" 2>/dev/null; }
pause() {
    echo
    read -r -p "  ↳ ${1:-press enter to continue}"
    echo
}
step() {
    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "═══════════════════════════════════════════════════════════════"
}
show_counts() {
    printf "  source Postgres          %s users / %s orders / %s products\n" \
        "$(pg -tAc 'SELECT COUNT(*) FROM users' | tr -d '\r')" \
        "$(pg -tAc 'SELECT COUNT(*) FROM orders' | tr -d '\r')" \
        "$(pg -tAc 'SELECT COUNT(*) FROM products' | tr -d '\r')"
    printf "  warehouse (via Trino)    %s users / %s orders / %s products\n" \
        "$(trino 'SELECT COUNT(*) FROM warehouse.public_users' | tr -d '\r"')" \
        "$(trino 'SELECT COUNT(*) FROM warehouse.public_orders' | tr -d '\r"')" \
        "$(trino 'SELECT COUNT(*) FROM warehouse.public_products' | tr -d '\r"')"
}

clear
cat <<'BANNER'
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║              warehouse-sync — interactive playground                 ║
║                                                                      ║
║  Five scenarios, one at a time. You see the source change, run the  ║
║  sync, and watch it land in the warehouse via Trino.                 ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
BANNER

step "0. Snapshot: where things stand right now"
show_counts

echo
echo "  If those two lines don't match, some earlier syncs are still catching"
echo "  up — that's fine, we'll sync again in a moment."
pause

step "1. append_only mode — insert new orders, watch them stream through"
echo "  Inserting 5 fresh orders into source..."
pg -c "INSERT INTO orders (user_id, amount_cents, status) SELECT (SELECT id FROM users ORDER BY random() LIMIT 1), (500 + floor(random()*5000))::bigint, 'placed' FROM generate_series(1,5);"
show_counts
echo
echo "  Source has 5 more orders than the warehouse. Now sync:"
pause "press enter to run: warehouse-sync run --config sync-configs/orders_sync.json"
$WS run --config sync-configs/orders_sync.json 2>&1 | grep '^\[' | tail
echo
show_counts
echo
echo "  See the state store record it:"
$WS inspect --sync-id orders_sync 2>&1 | grep -vE "(StatusLogger|WARN|Using|%d)" | tail -8
pause

step "2. merge mode — edit existing users, watch upsert-on-PK"
echo "  Renaming user id=1 in source..."
pg -c "UPDATE users SET display_name = 'Alice (edited at ' || now()::time(0)::text || ')' WHERE id = 1;"
echo "  Source now says:"
pg -c "SELECT id, email, display_name, updated_at FROM users WHERE id=1"
echo "  Warehouse still says:"
trino "SELECT id, email, display_name, updated_at FROM warehouse.public_users WHERE id=1"
pause "press enter to run merge sync"
$WS run --config sync-configs/users_sync.json 2>&1 | grep '^\[' | tail
echo
echo "  Warehouse after merge — same row, same PK, updated in place:"
trino "SELECT id, email, display_name, updated_at FROM warehouse.public_users WHERE id=1"
pause

step "3. full_replace mode — deactivate a product, watch snapshot swap"
echo "  Warehouse says active products:"
trino "SELECT id, sku, name, active FROM warehouse.public_products ORDER BY id"
echo
echo "  Deactivating SKU-1 in source..."
pg -c "UPDATE products SET active = false WHERE sku = 'SKU-1';"
echo "  Source now:"
pg -c "SELECT id, sku, name, active FROM products ORDER BY id"
pause "press enter to run full_replace sync"
$WS run --config sync-configs/products_full_refresh.json 2>&1 | grep '^\[' | tail
echo
echo "  Warehouse after full_replace — whole table snapshotted:"
trino "SELECT id, sku, name, active FROM warehouse.public_products ORDER BY id"
pause

step "4. dry-run — inspect a proposed sync without moving anything"
echo "  Add 3 more users in source but DON'T sync yet..."
pg -c "INSERT INTO users (email, display_name) SELECT 'dryrun_' || i || '@example.com', 'DryRunUser ' || i FROM generate_series(1,3) AS i;"
show_counts
echo
echo "  dry-run predicts what the next sync would do:"
pause "press enter to run: warehouse-sync dry-run --config sync-configs/users_sync.json"
$WS dry-run --config sync-configs/users_sync.json 2>&1 | grep -vE "(StatusLogger|WARN|Using|%d)" | tail -20
echo
echo "  Notice: no rows moved, watermark not advanced. Confirm:"
show_counts
pause

step "5. Iceberg time travel — query the warehouse AS OF the last snapshot"
echo "  Iceberg keeps every commit. List public_users snapshots:"
trino "SELECT committed_at, snapshot_id, summary['added-records'] AS rows_added, summary['deleted-records'] AS rows_deleted FROM warehouse.\"public_users\$snapshots\" ORDER BY committed_at DESC LIMIT 5"
echo
echo "  ...and the sync run history in the state store:"
trino "SELECT sync_id, started_at, status, rows_synced, watermark_before, watermark_after FROM sync_state.run_history ORDER BY started_at DESC LIMIT 8"
pause "final pause — press enter to see the wrap-up"

step "Wrap-up: what you just saw"
cat <<'END'
  • append_only  — orders streamed in, watermark advanced on `id` column
  • merge        — edited users upserted on primary key, no dupes
  • full_replace — products snapshotted whole, catches deletes and drift
  • dry-run      — safe preview, no state changed
  • state store  — every run recorded in Iceberg, queryable via Trino

  More to try on your own:
    ./bin/warehouse-sync backfill --config sync-configs/users_sync.json --reset
       ↳ wipes the watermark and re-pulls the entire source table

    ./bin/warehouse-sync serve --config-dir sync-configs/
       ↳ runs the daemon; every sync cadence tick pulls fresh rows.
         Insert into Postgres in another terminal to see it react.

    docker exec source-postgres psql -U sync -d source
       ↳ change anything — new schemas, new columns, weird types.
         Then dry-run to see how the sync would handle it.

    docker exec trino trino --catalog iceberg
       ↳ interactive SQL against the warehouse.
END
echo
END_MSG='  done — thanks for playing.'
echo "$END_MSG"
