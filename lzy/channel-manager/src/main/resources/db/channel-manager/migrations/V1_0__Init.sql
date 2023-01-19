CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    deadline        TIMESTAMP NULL,
    done            BOOLEAN   NOT NULL,

    meta            BYTEA     NULL,
    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE TYPE channel_operation_type AS ENUM ('BIND', 'UNBIND', 'DESTROY');

CREATE TABLE channel_operation
(
    op_id       TEXT                   NOT NULL PRIMARY KEY,
    started_at  TIMESTAMP              NOT NULL,
    deadline    TIMESTAMP              NOT NULL,

    op_type     channel_operation_type NOT NULL,
    state_json  TEXT                   NOT NULL, -- some operation specific data

-- operation failed, rollback required
    failed      BOOLEAN                NOT NULL DEFAULT FALSE,
    fail_reason TEXT                   NULL
);

CREATE TYPE channel_life_status_type AS ENUM ('ALIVE', 'DESTROYING');

CREATE TABLE channels
(
    channel_id    TEXT                     NOT NULL,
    execution_id  TEXT                     NOT NULL,
    workflow_name TEXT                     NOT NULL,
    user_id       TEXT                     NOT NULL,
    channel_name  TEXT                     NOT NULL,
    channel_spec  TEXT                     NOT NULL,

    life_status   channel_life_status_type NOT NULL,
    created_at    TIMESTAMP                NOT NULL,
    updated_at    TIMESTAMP                NOT NULL,

    CONSTRAINT channels_pkey PRIMARY KEY (channel_id)
);

CREATE UNIQUE INDEX channels_execution_id_channel_name_idx
    ON channels (execution_id, channel_name);

CREATE TYPE endpoint_life_status_type AS ENUM ('BINDING', 'BOUND', 'UNBINDING');

CREATE TABLE endpoints
(
    slot_uri    TEXT                      NOT NULL,
    "slot_name" TEXT                      NOT NULL,
    slot_owner  TEXT                      NOT NULL,
    task_id     TEXT                      NOT NULL,
    channel_id  TEXT                      NOT NULL,
    direction   TEXT                      NOT NULL,
    slot_spec   TEXT                      NOT NULL,

    life_status endpoint_life_status_type NOT NULL,
    created_at  TIMESTAMP                 NOT NULL,
    updated_at  TIMESTAMP                 NOT NULL,

    CONSTRAINT endpoints_pkey PRIMARY KEY (slot_uri),

    CONSTRAINT endpoints_channel_fkey
        FOREIGN KEY (channel_id) REFERENCES channels (channel_id)
            ON DELETE RESTRICT
);

CREATE UNIQUE INDEX endpoints_slot_name_task_id_idx
    ON endpoints ("slot_name", task_id);

CREATE TYPE connection_life_status_type AS ENUM ('CONNECTING', 'CONNECTED', 'DISCONNECTING');

CREATE TABLE IF NOT EXISTS connections
(
    sender_uri   TEXT                        NOT NULL,
    receiver_uri TEXT                        NOT NULL,
    channel_id   TEXT                        NOT NULL,

    life_status  connection_life_status_type NOT NULL,
    created_at   TIMESTAMP                   NOT NULL,
    updated_at   TIMESTAMP                   NOT NULL,

    CONSTRAINT connections_pkey PRIMARY KEY (sender_uri, receiver_uri),

    CONSTRAINT connections_sender_fkey
        FOREIGN KEY (sender_uri) REFERENCES endpoints (slot_uri)
            ON DELETE RESTRICT,

    CONSTRAINT connections_receiver_fkey
        FOREIGN KEY (receiver_uri) REFERENCES endpoints (slot_uri)
            ON DELETE RESTRICT
);
