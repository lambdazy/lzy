CREATE TYPE storage_type AS ENUM ('USER', 'INTERNAL');

CREATE TYPE portal_status AS ENUM (
    'CREATING_STD_CHANNELS', 'CREATING_SESSION',
    'REQUEST_VM', 'ALLOCATING_VM', 'VM_READY'
);

CREATE TYPE execution_status AS ENUM (
    'RUN', 'ERROR', 'COMPLETING', 'COMPLETED'
);

CREATE TABLE workflow_executions
(
    execution_id             TEXT             NOT NULL PRIMARY KEY,
    user_id                  TEXT             NOT NULL,
    execution_status         execution_status NOT NULL,

    allocator_session_id     TEXT,

    created_at               TIMESTAMP        NOT NULL,
    finished_at              TIMESTAMP,
    finished_with_error      TEXT,    -- error message or null
    finished_error_code      INTEGER, -- error code or null

    storage                  storage_type     NOT NULL,
    storage_uri              TEXT             NOT NULL,
    storage_credentials      TEXT             NOT NULL,

    portal                   portal_status,
    allocate_op_id           TEXT,
    portal_vm_id             TEXT,
    portal_vm_address        TEXT,
    portal_fs_address        TEXT,
    portal_id                TEXT,

    portal_stdout_channel_id TEXT,
    portal_stderr_channel_id TEXT,

    CHECK (finished_at >= created_at)
);

CREATE INDEX expired_workflow_executions_index ON workflow_executions (execution_status)
    WHERE execution_status = cast('ERROR' AS execution_status);

CREATE TABLE workflows
(
    user_id             TEXT      NOT NULL,
    workflow_name       TEXT      NOT NULL,
    created_at          TIMESTAMP NOT NULL,
    modified_at         TIMESTAMP NOT NULL,

    active_execution_id TEXT REFERENCES workflow_executions (execution_id),

    PRIMARY KEY (user_id, workflow_name)
);

CREATE TABLE snapshots
(
    slot_uri     TEXT NOT NULL PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES workflow_executions (execution_id)
);

CREATE TABLE channels
(
    channel_id      TEXT NOT NULL PRIMARY KEY,
    output_slot_uri TEXT NOT NULL REFERENCES snapshots (slot_uri),

    UNIQUE (output_slot_uri, channel_id)
);

CREATE TABLE graphs
(
    graph_id           TEXT   NOT NULL,
    execution_id       TEXT   NOT NULL REFERENCES workflow_executions (execution_id),
    portal_input_slots TEXT[] NOT NULL,

    PRIMARY KEY (graph_id, execution_id)
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

CREATE TABLE graph_op_state
(
    op_id      TEXT NOT NULL PRIMARY KEY,
    state_json TEXT NOT NULL, -- some operation specific state
    owner_id   TEXT NOT NULL, -- instance that created the op

    FOREIGN KEY (op_id) REFERENCES operation (id)
);

CREATE TABLE garbage_collectors
(
    gc_instance_id TEXT NOT NULL PRIMARY KEY,
    updated_at     TIMESTAMP,
    valid_until    TIMESTAMP
);
