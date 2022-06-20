CREATE TYPE disk_provider_type AS ENUM (
    'LOCAL_DIR',
    'S3_STORAGE'
);

CREATE TABLE IF NOT EXISTS disks
(
    disk_id       varchar(255)       NOT NULL,
    disk_provider disk_provider_type NOT NULL,
    disk_spec     jsonb              NOT NULL,
    created_at    timestamp          NOT NULL,

    CONSTRAINT disks_pkey PRIMARY KEY (disk_id)
);

CREATE TYPE disk_status_type AS ENUM (
    'PREPARING',
    'READY'
);

CREATE TABLE IF NOT EXISTS cached_envs
(
    env_id        varchar(255)     NOT NULL,
    workflow_name varchar(255)     NOT NULL,
    disk_id       varchar(255)     NOT NULL,
    created_at    timestamp        NOT NULL,
    status        disk_status_type NOT NULL,
    docker_image  varchar(255)     NOT NULL,
    yaml_config   varchar(255)     NOT NULL,
    updated_at    timestamp        NOT NULL,

    CONSTRAINT cached_envs_pkey
        PRIMARY KEY (env_id),

    CONSTRAINT cached_envs_workflow_name_disk_id_unique
        UNIQUE (workflow_name, disk_id),

    CONSTRAINT cached_envs_disk_id_fkey
        FOREIGN KEY (disk_id) REFERENCES disks(disk_id)
            ON DELETE CASCADE
);

