create type storage_type as enum ('user', 'internal');

create type portal_status as enum (
    'not_started', 'creating_session', 'request_vm',
    'allocating_vm', 'vm_ready'
);

create table workflow_executions (
    execution_id varchar(255) not null,

    session_id varchar(255),

    created_at timestamp not null,
    finished_at timestamp,
    finished_with_error varchar(255),  -- error message or null

    storage storage_type not null,
    storage_bucket varchar(255) not null,
    storage_credentials varchar(1048576) not null,

    portal portal_status not null,
    allocate_op_id varchar(255),
    portal_vm_id varchar(255),
    portal_vm_address varchar(1024),

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
