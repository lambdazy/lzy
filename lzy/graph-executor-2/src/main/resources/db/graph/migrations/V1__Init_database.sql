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

CREATE TYPE status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE graph
(
    id                    TEXT                           NOT NULL PRIMARY KEY,
    op_id                 TEXT                           NOT NULL REFERENCES operation (id) ON DELETE CASCADE,
    status                status default 'WAITING'       NOT NULL,
    workflow_id           TEXT                           NOT NULL,
    workflow_name         TEXT                           NOT NULL,
    user_id               TEXT                           NOT NULL,
    error_description     TEXT                           NULL,
    failed_task_id        TEXT                           NULL,
    failed_task_name      TEXT                           NULL,
    last_updated          TIMESTAMP                      NOT NULL,
    owner_instance_id     TEXT                           NOT NULL
);

CREATE TYPE task_status AS ENUM ('WAITING', 'WAITING_ALLOCATION', 'ALLOCATING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE task (
    id                    TEXT                          NOT NULL PRIMARY KEY,
    op_id                 TEXT                          NOT NULL REFERENCES operation (id) ON DELETE CASCADE,
    task_name             TEXT                          NOT NULL,
    graph_id              TEXT                          NOT NULL REFERENCES graph (id) ON DELETE CASCADE,
    status                task_status default 'WAITING' NOT NULL,
    workflow_id           TEXT                          NOT NULL,
    workflow_name         TEXT                          NOT NULL,
    user_id               TEXT                          NOT NULL,
    task_description      TEXT                          NULL,
    task_state            TEXT                          NULL,
    error_description     TEXT                          NULL,
    owner_instance_id     TEXT                          NOT NULL,
    alloc_session         TEXT                          NULL
);

CREATE TABLE task_dependency (
    id                    SERIAL    PRIMARY KEY,
    task_id               TEXT      NOT NULL REFERENCES task (id) ON DELETE CASCADE,
    dependent_task_id     TEXT      NOT NULL REFERENCES task (id) ON DELETE CASCADE
);