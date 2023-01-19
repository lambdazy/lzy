create type storage_type as enum ('USER', 'INTERNAL');

create type portal_status as enum (
    'CREATING_STD_CHANNELS', 'CREATING_SESSION',
    'REQUEST_VM', 'ALLOCATING_VM', 'VM_READY'
);

create type execution_status as enum (
    'RUN', 'COMPLETED', 'ERROR', 'CLEANED'
);

create table workflow_executions
(
    execution_id             text             not null,
    execution_status         execution_status not null,

    allocator_session_id     text,

    created_at               timestamp        not null,
    finished_at              timestamp,
    finished_with_error      text,    -- error message or null
    finished_error_code      integer, -- error code or null

    storage                  storage_type     not null,
    storage_uri              text             not null,
    storage_credentials      text             not null,

    portal                   portal_status,
    allocate_op_id           text,
    portal_vm_id             text,
    portal_vm_address        text,
    portal_fs_address        text,
    portal_id                text,

    portal_stdout_channel_id text,
    portal_stderr_channel_id text,

    primary key (execution_id),
    check (finished_at >= created_at)
);

CREATE INDEX expired_workflow_executions_index ON workflow_executions (execution_status) WHERE execution_status = cast('ERROR' as execution_status);

create table workflows
(
    user_id             text      not null,
    workflow_name       text      not null,
    created_at          timestamp not null,

    active_execution_id text,

    primary key (user_id, workflow_name),
    foreign key (active_execution_id) references workflow_executions (execution_id)
);

create table snapshots
(
    slot_uri     text not null,
    execution_id text not null,

    primary key (slot_uri),
    foreign key (execution_id) references workflow_executions (execution_id)
);

create table channels
(
    channel_id      text not null,
    output_slot_uri text not null,

    primary key (channel_id),
    foreign key (output_slot_uri) references snapshots (slot_uri),
    unique (output_slot_uri, channel_id)
);

create table graphs
(
    graph_id           text not null,
    execution_id       text not null,
    portal_input_slots text[] not null,
    primary key (graph_id, execution_id),
    foreign key (execution_id) references workflow_executions (execution_id)
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

create table garbage_collectors
(
    gc_instance_id text not null,
    updated_at     timestamp,
    valid_until    timestamp,
    primary key (gc_instance_id)
);
