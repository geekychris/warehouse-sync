#!/usr/bin/env bash
# Nuke local state, rebuild the jar, restart the stack. Mirrors
# structured-logger's reset-and-rebuild.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_DIR="${SPARK_MINIO_TRINO_DIR:-$PROJECT_DIR/../spark_minio_trino}"

cd "$PROJECT_DIR"

echo "==> Tearing down stack (destroys source-postgres volume)"
docker compose \
    -f "$STACK_DIR/docker-compose.yml" \
    -f docker-compose.yml \
    -f lakehouse-override.yml \
    down -v || true

echo "==> Cleaning Maven target/"
if command -v mvn >/dev/null 2>&1; then
    mvn -q clean
fi

echo "==> Rebuilding jar"
mvn -q -DskipTests package

echo "==> Restarting stack"
"$SCRIPT_DIR/start-stack.sh"

echo ""
echo "Reset complete. Try:"
echo "  ./scripts/seed-postgres.sh"
echo "  java -jar target/warehouse-sync-*-all.jar validate-config --config sync-configs/users_sync.json"
