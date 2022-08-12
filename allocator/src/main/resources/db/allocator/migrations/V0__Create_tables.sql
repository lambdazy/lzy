CREATE TABLE session (
    id varchar(255) NOT NULL PRIMARY KEY,
    owner varchar(255) NOT NULL,
    cache_policy_json varchar(10485760) NOT NULL
);

CREATE TABLE vm (
    id varchar(255) NOT NULL PRIMARY KEY,
    session_id varchar(255) NOT NULL,
    pool_label varchar(255) NOT NULL,
    zone varchar(255) NOT NULL,
    state varchar(255) NOT NULL,
    allocation_op_id varchar(255) NOT NULL,
    workloads_json varchar(10485760) NOT NULL,

    last_activity_time timestamp NULL,
    deadline timestamp NULL,
    allocation_deadline timestamp NULL,
    allocator_meta_json varchar(10485760) NULL,
    vm_meta_json varchar(10485760) NULL
);

CREATE TABLE operation (
    id varchar(255) NOT NULL PRIMARY KEY,
    meta bytea NOT NULL,
    created_by varchar(255) NOT NULL,
    created_at timestamp NOT NULL,
    modified_at timestamp NOT NULL,
    description varchar(4096) NOT NULL,
    done bool NOT NULL,

    response bytea NULL,
    error bytea NULL
)
