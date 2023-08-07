ALTER TABLE peers
    ADD COLUMN idempotency_key      TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN request_hash         TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN conn_idempotency_key TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN conn_request_hash    TEXT NOT NULL DEFAULT 'default';

ALTER TABLE transfers
    ADD COLUMN state_change_idk TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN idempotency_key  TEXT NULL UNIQUE,
    ADD COLUMN request_hash     TEXT NOT NULL DEFAULT 'default';
