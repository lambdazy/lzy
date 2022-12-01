create type storage_type as enum ('USER', 'INTERNAL');

create type portal_status as enum (
    'CREATING_STD_CHANNELS', 'CREATING_SESSION',
    'REQUEST_VM', 'ALLOCATING_VM', 'VM_READY'
);

create type execution_status as enum (
    'RUN', 'COMPLETED', 'ERROR', 'CLEANED'
);

create table workflow_executions (
    execution_id text not null,
    execution_status execution_status not null,

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
    portal_fs_address text,
    portal_id text,

    portal_stdout_channel_id text,
    portal_stderr_channel_id text,

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

create table snapshots (
    slot_uri text not null,
    execution_id text not null,

    primary key (slot_uri),
    foreign key (execution_id) references workflow_executions (execution_id)
);

create table channels (
    channel_id text not null,
    output_slot_uri text not null,

    primary key (channel_id),
    foreign key (output_slot_uri) references snapshots (slot_uri),
    unique (output_slot_uri, channel_id)
);

create table graphs (
    graph_id text not null,
    execution_id text not null,
    portal_input_slots text[] not null,
    primary key (graph_id, execution_id),
    foreign key (execution_id) references workflow_executions(execution_id)
);

create table garbage_collectors (
    gc_instance_id text not null,
    updated_at timestamp,
    valid_until timestamp,
    primary key (gc_instance_id)
);