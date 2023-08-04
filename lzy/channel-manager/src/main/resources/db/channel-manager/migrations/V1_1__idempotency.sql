ALTER TABLE peers
    ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN request_hash    TEXT NOT NULL DEFAULT 'default';

ALTER TABLE transfers
    ADD COLUMN idempotency_key TEXT NULL UNIQUE,
    ADD COLUMN request_hash    TEXT NULL,
    ADD CONSTRAINT idk_req_hash CHECK
        (idempotency_key IS NULL AND request_hash IS NULL OR idempotency_key IS NOT NULL AND request_hash IS NOT NULL);
