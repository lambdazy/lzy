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
    id                   text      NOT NULL PRIMARY KEY,
    channel_id           text      NOT NULL,
    "role"               text      NOT NULL,  /* PRODUCER or CONSUMER*/
    peer_description     text      NOT NULL,
    priority             integer   NOT NULL,
    connected            boolean   NOT NULL,

    FOREIGN KEY(channel_id) references channels(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE pending_transfers
(
    slot_id        text       NOT NULL,
    peer_id        text       NOT NULL,
    PRIMARY KEY (slot_id, peer_id),
    FOREIGN KEY (slot_id)   references peers(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (peer_id)   references peers(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);