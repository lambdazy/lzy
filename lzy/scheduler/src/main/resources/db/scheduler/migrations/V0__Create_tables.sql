CREATE TABLE task (
    id                    text      NOT NULL,
    execution_id          text      NOT NULL,
    workflow_name         text      NOT NULL,
    user_id               text      NOT NULL,
    task_description_json text      NOT NULL,
    status                text      NOT NULL,
    last_activity_time    timestamp NOT NULL,

    rc                    int       NULL,
    error_description     text      NULL,
    allocator_op_id       text      NULL,
    vm_id                 text      NULL,
    PRIMARY KEY(id, execution_id)
);

CREATE TABLE workflow (
    name                 text NOT NULL,
    user_id              text NOT NULL,
    allocator_session_id text NULL,
    PRIMARY KEY(name, user_id)
)