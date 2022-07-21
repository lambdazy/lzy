create table workflow_executions (
    execution_id varchar(255) not null,

    created_at timestamp not null,
    finished_at timestamp,
    finished_with_error varchar(255),  -- error message or null

    storage_bucket varchar(255) not null,

    primary key (execution_id)
);

create table workflows (
    user_id varchar(255) not null,
    workflow_name varchar(255) not null,
    created_at timestamp not null,

    active_execution_id varchar(255),

    primary key (user_id, workflow_name),
    foreign key (active_execution_id) references workflow_executions (execution_id)
);

