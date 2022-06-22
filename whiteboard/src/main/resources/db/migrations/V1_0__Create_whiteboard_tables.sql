CREATE TABLE snapshot (
    snapshot_id     text          PRIMARY KEY,
    snapshot_state  varchar(20)   NOT NULL
);

CREATE TABLE whiteboard (
    wb_id         text         PRIMARY KEY,
    snapshot_id   text         NOT NULL REFERENCES snapshot (snapshot_id) ON UPDATE CASCADE ON DELETE CASCADE,
    wb_state      varchar(20)  NOT NULL
);

CREATE TABLE snapshot_entry (
    snapshot_id  text         REFERENCES snapshot (snapshot_id) ON UPDATE CASCADE ON DELETE CASCADE,
    entry_id     text,
    storage_uri  text,
    empty        boolean      NOT NULL,
    state        varchar(20),
    PRIMARY KEY (snapshot_id, entry_id)
);

CREATE TABLE entry_dependencies (
    snapshot_id    text  REFERENCES snapshot (snapshot_id) ON UPDATE CASCADE ON DELETE CASCADE,
    entry_id_from  text,
    entry_id_to    text,
    PRIMARY KEY (snapshot_id, entry_id_from, entry_id_to)
);

CREATE TABLE whiteboard_field (
    wb_id       text  REFERENCES whiteboard (wb_id) ON UPDATE CASCADE ON DELETE CASCADE,
    field_name  text,
    entry_id    text,
    PRIMARY KEY (wb_id, field_name)
)