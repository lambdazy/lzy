create table if not exists workflows (
    user_id varchar(255) not null ,
    workflow_name varchar(255) not null,
    created_at timestamp not null,

    execution_id varchar(255),
    execution_started_at timestamp,

    primary key (user_id, workflow_name)
);

-- create table if not exists portals (
--     workflow_execution_id varchar(255) not null,
--     portal_servant_id varchar(255) not null,
--     portal_address varchar(255) not null, -- grpc endpoint in `host:port` format
--     created_at timestamp not null,
--
--     primary key (workflow_execution_id)
-- );
