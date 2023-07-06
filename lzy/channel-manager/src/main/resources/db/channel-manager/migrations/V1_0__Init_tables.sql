CREATE TABLE channels
(
    id                   text      NOT NULL PRIMARY KEY,
    owner_id             text      NOT NULL,
    execution_id         text      NOT NULL,
    workflow_name        text      NOT NULL,
    data_scheme_json     text      NULL,
    storage_producer_uri text      NULL,
    storage_consumer_uri text      NULL
);

CREATE TABLE peers
(
    id                   text      NOT NULL,
    channel_id           text      NOT NULL,
    "role"               text      NOT NULL,  /* PRODUCER or CONSUMER*/
    description          text      NOT NULL,
    priority             integer   NOT NULL,
    connected            boolean   NOT NULL,

    PRIMARY KEY (id, channel_id),

    FOREIGN KEY(channel_id) references channels(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE transfers
(
    id                text       NOT NULL,
    channel_id        text       NOT NULL,
    from_id           text       NOT NULL,
    to_id             text       NOT NULL,
    state             text       NOT NULL,  /* PENDING, ACTIVE, FAILED, COMPLETED */
    error_description text       NULL,      /* Error description if transfer is failed */

    PRIMARY KEY (id, channel_id),

    FOREIGN KEY (from_id, channel_id)    references peers(id, channel_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (to_id, channel_id)      references peers(id, channel_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (channel_id) references channels(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);