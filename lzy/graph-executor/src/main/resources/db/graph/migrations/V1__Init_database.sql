CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE graph_execution_state
(
    workflow_id                  TEXT                                     NOT NULL,
    workflow_name                TEXT                                     NOT NULL,
    user_id                      TEXT                                     NOT NULL,
    id                           TEXT                                     NOT NULL,
    error_description            TEXT                                     NULL,
    failed_task                  TEXT                                     NULL,
    status                       graph_execution_status default 'WAITING' NOT NULL,

    graph_description_json       TEXT                                     NOT NULL,
    task_executions_json         TEXT                                     NOT NULL,
    current_execution_group_json TEXT                                     NOT NULL,

    last_updated                 TIMESTAMP                                NOT NULL,
    acquired                     BOOLEAN                                  NOT NULL,

    PRIMARY KEY (workflow_id, id)
);

CREATE TYPE event_type AS ENUM ('START', 'STOP');

CREATE TABLE queue_event
(
    id          TEXT       NOT NULL PRIMARY KEY,

    type        event_type NOT NULL,
    workflow_id TEXT       NOT NULL,
    graph_id    TEXT       NOT NULL,
    acquired    BOOLEAN    NOT NULL,
    description TEXT       NOT NULL,

    FOREIGN KEY (workflow_id, graph_id) REFERENCES graph_execution_state (workflow_id, id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    meta            BYTEA     NULL,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    deadline        TIMESTAMP NULL,
    done            BOOLEAN   NOT NULL,

    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
