CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    deadline        TIMESTAMP NULL,
    done            BOOLEAN   NOT NULL,

    meta            BYTEA     NULL,
    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
CREATE UNIQUE INDEX failed_operations_index ON operation (id) WHERE done = TRUE AND error IS NOT NULL;
CREATE UNIQUE INDEX completed_operations_index ON operation (id) WHERE done = TRUE;

CREATE TYPE task_status AS ENUM ('PENDING', 'RUNNING', 'FAILED', 'FINISHED');

CREATE TYPE task_type AS ENUM ('UNMOUNT', 'MOUNT');

CREATE TABLE IF NOT EXISTS operation_task(
    id              BIGSERIAL   NOT NULL,
    name            TEXT        NOT NULL,
    entity_id       TEXT        NOT NULL,
    type            task_type   NOT NULL,
    status          task_status NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    updated_at      TIMESTAMP   NOT NULL,
    metadata        JSONB       NOT NULL,
    operation_id    TEXT,
    worker_id       TEXT,
    lease_till      TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (operation_id) REFERENCES operation(id)
);