CREATE TYPE disk_provider_type AS ENUM (
    'LOCAL_DIR',
    'S3_STORAGE'
);

CREATE TABLE IF NOT EXISTS disks
(
    disk_id         varchar(255)        NOT NULL,
    user_id         varchar(255)        NOT NULL,
    disk_provider   disk_provider_type  NOT NULL,
    disk_spec_json  varchar(10485760)   NOT NULL,
    created_at      timestamp           NOT NULL,

    CONSTRAINT disks_pkey PRIMARY KEY (disk_id)
);
