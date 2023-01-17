CREATE TABLE task (
    id                    text      NOT NULL,
    execution_id          text      NOT NULL,
    workflow_name         text      NOT NULL,
    user_id               text      NOT NULL,
    operation_id          text      NOT NULL,
    PRIMARY KEY(id, execution_id)
);

CREATE TABLE workflow (
    name                 text NOT NULL,
    user_id              text NOT NULL,
    allocator_session_id text NULL,
    PRIMARY KEY(name, user_id)
)