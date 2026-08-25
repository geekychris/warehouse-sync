#!/usr/bin/env bash
# End-to-end smoke test that mirrors structured-logger's demo-end-to-end.sh:
#   1. bring up the stack
#   2. seed some source data
#   3. validate configs
#   4. dry-run each config
#   5. run each config (writes to warehouse)
#   6. seed some more churn
#   7. run again (proves incremental)
#   8. inspect state
#   9. query the warehouse via Trino
#
# Prereqs: docker, java, mvn, and $SPARK_MINIO_TRINO_DIR pointing at a
# checkout of spark_minio_trino.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Env for the JVM to reach the local stack + source db.
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

cd "$PROJECT_DIR"

echo "==> 1. bringing up stack"
./scripts/start-stack.sh

echo "==> 2. build jar"
mvn -q -DskipTests package

JAR="$PROJECT_DIR/target/warehouse-sync-0.1.0-all.jar"

echo "==> 3. seed source (initial)"
./scripts/seed-postgres.sh

echo "==> 4. validate configs"
java -jar "$JAR" validate-config --config sync-configs/

echo "==> 5. dry-run all configs"
java -jar "$JAR" dry-run --config sync-configs/

echo "==> 6. first sync run"
java -jar "$JAR" run --config sync-configs/

echo "==> 7. seed churn"
./scripts/seed-postgres.sh

echo "==> 8. second sync run (should be incremental)"
java -jar "$JAR" run --config sync-configs/

echo "==> 9. inspect state"
java -jar "$JAR" inspect

echo "==> 10. query warehouse via Trino"
./scripts/query-warehouse.sh iceberg "SHOW TABLES FROM iceberg.warehouse"
./scripts/query-warehouse.sh iceberg "SELECT COUNT(*) FROM iceberg.warehouse.public_users"
./scripts/query-warehouse.sh iceberg "SELECT COUNT(*) FROM iceberg.warehouse.public_orders"
./scripts/query-warehouse.sh iceberg "SELECT * FROM iceberg.warehouse.public_products"

echo ""
echo "✓ demo complete"
