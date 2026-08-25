#!/usr/bin/env bash
# Quick Trino query helper for the warehouse tables the sync engine writes to.
set -euo pipefail

: "${TRINO_HOST:=127.0.0.1}"
: "${TRINO_PORT:=18013}"
CATALOG="${1:-iceberg}"
QUERY="${2:-SHOW TABLES FROM ${CATALOG}.warehouse}"

if command -v trino >/dev/null 2>&1; then
    trino --server "http://${TRINO_HOST}:${TRINO_PORT}" --catalog "$CATALOG" --execute "$QUERY"
elif command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -q '^trino$'; then
    docker exec -i trino trino --catalog "$CATALOG" --execute "$QUERY"
else
    echo "ERROR: no trino CLI on PATH and no 'trino' container running." >&2
    exit 1
fi
