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
    op_id                 TEXT                           NOT NULL REFERENCES operation (id),
    status                status default 'WAITING'       NOT NULL,
    workflow_id           TEXT                           NOT NULL,
    workflow_name         TEXT                           NOT NULL,
    user_id               TEXT                           NOT NULL,
    graph_description     TEXT                           NOT NULL,
    error_description     TEXT                           NULL,
    last_updated          TIMESTAMP                      NOT NULL,
    owner_instance_id     TEXT                           NOT NULL
);

CREATE TABLE task (
    id                    TEXT                          NOT NULL PRIMARY KEY,
    graph_id              TEXT                          NOT NULL REFERENCES graph (id),
    status                status default 'WAITING'      NOT NULL,
    workflow_id           TEXT                          NOT NULL,
    workflow_name         TEXT                          NOT NULL,
    user_id               TEXT                          NOT NULL,
    task_description      TEXT                          NOT NULL,
    owner_instance_id     TEXT                          NOT NULL
);

CREATE TABLE task_dependency (
    id                    TEXT      NOT NULL PRIMARY KEY,
    task_id               TEXT      NOT NULL REFERENCES task (id),
    dependent_task_id     TEXT      NOT NULL REFERENCES task (id)
);

CREATE TYPE task_op_status AS ENUM ('WAITING', 'ALLOCATING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE task_operation
(
    id                    TEXT                             NOT NULL PRIMARY KEY,
    task_id               TEXT                             NOT NULL REFERENCES task (id),
    started_at            TIMESTAMP                        NOT NULL,
    deadline              TIMESTAMP                        NOT NULL,
    owner_instance_id     TEXT                             NOT NULL,
    status                task_op_status default 'WAITING' NOT NULL,
    task_state            TEXT                             NOT NULL,
    error_description     TEXT                             NULL
);