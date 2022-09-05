create type storage_type as enum ('USER', 'INTERNAL');

create type portal_status as enum (
    'CREATING_SESSION', 'REQUEST_VM',
    'ALLOCATING_VM', 'VM_READY'
);

create table workflow_executions (
    execution_id text not null,

    allocator_session_id text,

    created_at timestamp not null,
    finished_at timestamp,
    finished_with_error text,  -- error message or null

    storage storage_type not null,
    storage_bucket text not null,
    storage_credentials text not null,

    portal portal_status,
    allocate_op_id text,
    portal_vm_id text,
    portal_vm_address text,

    primary key (execution_id),
    check (finished_at >= created_at)
);

create table workflows (
    user_id text not null,
    workflow_name text not null,
    created_at timestamp not null,

    active_execution_id text,

    primary key (user_id, workflow_name),
    foreign key (active_execution_id) references workflow_executions (execution_id)
);
