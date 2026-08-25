#!/usr/bin/env bash
# Generate realistic churn against the source Postgres so you can watch
# incremental syncs move rows. Safe to run repeatedly.
set -euo pipefail

: "${SOURCE_PG_HOST:=127.0.0.1}"
: "${SOURCE_PG_PORT:=15433}"
: "${SOURCE_PG_DB:=source}"
: "${SOURCE_PG_USER:=sync}"
: "${SOURCE_PG_PASSWORD:=sync}"

PSQL_URL="postgresql://${SOURCE_PG_USER}:${SOURCE_PG_PASSWORD}@${SOURCE_PG_HOST}:${SOURCE_PG_PORT}/${SOURCE_PG_DB}"

# Prefer psql; fall back to docker exec if not installed.
if command -v psql >/dev/null 2>&1; then
    RUN_PSQL=(psql "$PSQL_URL" -v ON_ERROR_STOP=1)
else
    RUN_PSQL=(docker exec -i source-postgres psql -U "$SOURCE_PG_USER" -d "$SOURCE_PG_DB" -v ON_ERROR_STOP=1)
fi

echo "Seeding source Postgres..."
"${RUN_PSQL[@]}" <<'SQL'
-- New users
INSERT INTO users (email, display_name)
SELECT
    'user_' || floor(random() * 1e9)::bigint || '@example.com',
    'User ' || floor(random() * 1e6)::bigint
FROM generate_series(1, 5);

-- Rename some existing users (bumps updated_at → picked up by merge sync)
UPDATE users
SET display_name = display_name || ' (edited ' || now()::text || ')'
WHERE id IN (SELECT id FROM users ORDER BY random() LIMIT 3);

-- New orders (append_only path)
INSERT INTO orders (user_id, amount_cents, status)
SELECT
    (SELECT id FROM users ORDER BY random() LIMIT 1),
    (100 + floor(random() * 10000))::bigint,
    (ARRAY['placed','shipped','cancelled'])[1 + floor(random()*3)::int]
FROM generate_series(1, 20);

-- Deactivate a product (full_replace path — merge would miss this without a
-- deleted_at column; full_replace catches it because it snapshots everything)
UPDATE products SET active = FALSE
WHERE id = (SELECT id FROM products WHERE active ORDER BY random() LIMIT 1);
SQL

"${RUN_PSQL[@]}" -c "SELECT (SELECT COUNT(*) FROM users) AS users, (SELECT COUNT(*) FROM orders) AS orders, (SELECT COUNT(*) FROM products) AS products;"
