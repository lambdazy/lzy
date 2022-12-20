CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE graph_execution_state
(
    workflow_id                  varchar(255)                             NOT NULL,
    workflow_name                varchar(255)                             NOT NULL,
    user_id                      varchar(255)                             NOT NULL,
    id                           varchar(255)                             NOT NULL,
    error_description            text                                     NULL,
    status                       graph_execution_status default 'WAITING' NOT NULL,

    graph_description_json       text                                     NOT NULL,
    task_executions_json         text                                     NOT NULL,
    current_execution_group_json text                                     NOT NULL,

    last_updated                 timestamp                                NOT NULL,
    acquired                     bool                                     NOT NULL,

    primary key (workflow_id, id)
);

CREATE TYPE event_type AS ENUM ('START', 'STOP');

CREATE TABLE queue_event
(
    id          varchar(255) NOT NULL PRIMARY KEY,

    type        event_type   NOT NULL,
    workflow_id varchar(255) NOT NULL,
    graph_id    varchar(255) NOT NULL,
    acquired    bool         NOT NULL,
    description text         NOT NULL,

    foreign key (workflow_id, graph_id) REFERENCES graph_execution_state (workflow_id, id)
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
    done            bool      NOT NULL,

    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
