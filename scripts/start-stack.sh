#!/usr/bin/env bash
# Bring up the shared lakehouse stack + warehouse-sync's demo source Postgres.
# Mirrors the structured-logger start-* pattern.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_DIR="${SPARK_MINIO_TRINO_DIR:-$PROJECT_DIR/../spark_minio_trino}"

if [ ! -f "$STACK_DIR/docker-compose.yml" ]; then
    echo "ERROR: shared lakehouse stack not found at $STACK_DIR" >&2
    echo "  Set SPARK_MINIO_TRINO_DIR to point at your checkout of spark_minio_trino." >&2
    exit 1
fi

cd "$PROJECT_DIR"

echo "Bringing up shared lakehouse + source-postgres..."
# Order: base spark_minio_trino compose first (so ITS relative paths like
# ./Dockerfile.hive-metastore resolve inside that repo); our compose uses
# ${PWD} for volume mounts to escape docker compose's "relative to first -f"
# rule.
docker compose \
    -f "$STACK_DIR/docker-compose.yml" \
    -f docker-compose.yml \
    -f lakehouse-override.yml \
    up -d

echo ""
echo "Stack up. Endpoints:"
echo "  MinIO console      http://localhost:18001    (admin/password123)"
echo "  Trino              http://localhost:18013"
echo "  Spark master UI    http://localhost:18082"
echo "  Source Postgres    postgres://sync:sync@localhost:15433/source"
echo ""
echo "Next:"
echo "  ./scripts/seed-postgres.sh          # generate some churn to sync"
echo "  ./scripts/query-warehouse.sh        # peek at the warehouse via Trino"
