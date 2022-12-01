CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    done            bool      NOT NULL,

    meta            BYTEA     NULL,
    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE TYPE channel_operation_type AS ENUM ('BIND', 'UNBIND', 'DESTROY');

CREATE TABLE channel_operation
(
    op_id       TEXT                    NOT NULL PRIMARY KEY,
    started_at  TIMESTAMP               NOT NULL,
    deadline    TIMESTAMP               NOT NULL,

    op_type     channel_operation_type  NOT NULL,
    meta_json   TEXT                    NOT NULL, -- some operation specific data

-- operation failed, rollback required
    failed      BOOLEAN                 NOT NULL DEFAULT FALSE,
    fail_reason TEXT                    NULL
);