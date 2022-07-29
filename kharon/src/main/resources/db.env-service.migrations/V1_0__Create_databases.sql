CREATE TYPE disk_status_type AS ENUM (
    'PREPARING',
    'READY'
);

CREATE TABLE IF NOT EXISTS cached_envs
(
    env_id        varchar(255)     NOT NULL,
    user_id       varchar(255)     NOT NULL,
    workflow_name varchar(255)     NOT NULL,
    disk_id       varchar(255)     NOT NULL,
    created_at    timestamp        NOT NULL,
    status        disk_status_type NOT NULL,
    docker_image  varchar(255)     NOT NULL,
    conda_yaml    varchar(255)     NOT NULL,
    updated_at    timestamp        NOT NULL,

    CONSTRAINT cached_envs_pkey
        PRIMARY KEY (env_id),

    CONSTRAINT cached_envs_workflow_name_disk_id_unique
        UNIQUE (user_id, workflow_name, disk_id)

);

