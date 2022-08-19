CREATE TABLE IF NOT EXISTS channels (
    channel_id     varchar(255)      NOT NULL,
    user_id        varchar(255)      NOT NULL,
    workflow_id    varchar(255)      NOT NULL,
    channel_name   varchar(255)      NOT NULL,
    channel_type   varchar(255)      NOT NULL,
    channel_spec   varchar(10485760) NOT NULL,
    created_at     timestamp         NOT NULL,
    channel_life_status varchar(255) NOT NULL,

    CONSTRAINT channels_pkey PRIMARY KEY (channel_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS user_id_workflow_id_channel_name_idx
    ON channels(user_id, workflow_id, channel_name);


CREATE TABLE IF NOT EXISTS channel_endpoints (
    channel_id varchar(255)      NOT NULL,
    slot_uri   varchar(255)      NOT NULL,
    direction  varchar(255)      NOT NULL,
    task_id    varchar(255)      NOT NULL,
    slot_spec  varchar(10485760) NOT NULL,

    CONSTRAINT channel_endpoints_pkey PRIMARY KEY (channel_id, slot_uri),

    CONSTRAINT channel_endpoints_channel_id_fkey
        FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS endpoint_connections (
    channel_id   varchar(255)      NOT NULL,
    sender_uri   varchar(255)      NOT NULL,
    receiver_uri varchar(255)      NOT NULL,

    CONSTRAINT endpoint_connections PRIMARY KEY (channel_id, sender_uri, receiver_uri),

    CONSTRAINT endpoint_connections_sender_fkey
        FOREIGN KEY (channel_id, sender_uri) REFERENCES channel_endpoints(channel_id, slot_uri)
        ON DELETE CASCADE,

    CONSTRAINT endpoint_connections_receiver_fkey
        FOREIGN KEY (channel_id, receiver_uri) REFERENCES channel_endpoints(channel_id, slot_uri)
        ON DELETE CASCADE
);

