create table if not exists workflows (
    user_id varchar(255) not null ,
    workflow_name varchar(255) not null,
    created_at timestamp not null,

    execution_id varchar(255),
    execution_started_at timestamp,

    PRIMARY KEY (user_id, workflow_name)
);
