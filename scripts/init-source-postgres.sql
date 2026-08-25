-- Schema for the demo source Postgres. Loaded automatically the first time
-- the container starts (via /docker-entrypoint-initdb.d).
--
-- Three tables that exercise the three write modes shipped in v1:
--   users     → merge          (updates in place, PK id)
--   orders    → append_only    (immutable rows, id monotonically increases)
--   products  → full_replace   (small reference table, catches deletes)

CREATE TABLE IF NOT EXISTS users (
    id           BIGSERIAL   PRIMARY KEY,
    email        TEXT        NOT NULL UNIQUE,
    display_name TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

CREATE TABLE IF NOT EXISTS orders (
    id           BIGSERIAL   PRIMARY KEY,
    user_id      BIGINT      NOT NULL REFERENCES users(id),
    amount_cents BIGINT      NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'placed',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL PRIMARY KEY,
    sku         TEXT      NOT NULL UNIQUE,
    name        TEXT      NOT NULL,
    price_cents BIGINT    NOT NULL,
    active      BOOLEAN   NOT NULL DEFAULT TRUE
);

-- Trigger that bumps users.updated_at whenever the row changes — this is
-- the mechanism the incremental watermark relies on for the merge-mode demo.
CREATE OR REPLACE FUNCTION touch_users_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION touch_users_updated_at();

-- A pinch of seed data so `warehouse-sync run --all` on a fresh stack has
-- something to move.
INSERT INTO users (email, display_name) VALUES
    ('alice@example.com', 'Alice'),
    ('bob@example.com',   'Bob'),
    ('carol@example.com', 'Carol')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (sku, name, price_cents, active) VALUES
    ('SKU-1', 'Widget',       999,  TRUE),
    ('SKU-2', 'Gadget',       1999, TRUE),
    ('SKU-3', 'Retired Item', 500,  FALSE)
ON CONFLICT (sku) DO NOTHING;

INSERT INTO orders (user_id, amount_cents, status) VALUES
    (1, 999,  'placed'),
    (2, 2998, 'shipped'),
    (1, 500,  'placed');
