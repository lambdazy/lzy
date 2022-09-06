CREATE TABLE IF NOT EXISTS whiteboards (
    whiteboard_id        varchar(255)  NOT NULL,
    whiteboard_name      varchar(255)  NOT NULL,
    user_id              varchar(255)  NOT NULL,
    storage_name         varchar(255)  NOT NULL,
    storage_description  text          NOT NULL,
    namespace            varchar(255)  NOT NULL,
    whiteboard_status    varchar(255)  NOT NULL,
    created_at           timestamp     NOT NULL,
    finalized_at         timestamp,

    CONSTRAINT whiteboards_pkey PRIMARY KEY (whiteboard_id)
);

CREATE INDEX IF NOT EXISTS whiteboards_user_id_idx ON whiteboards(user_id);

CREATE TABLE IF NOT EXISTS whiteboard_tags (
    whiteboard_id   varchar(255)  NOT NULL,
    whiteboard_tag  varchar(255)  NOT NULL,

    CONSTRAINT whiteboard_tags_pkey PRIMARY KEY (whiteboard_id, whiteboard_tag),

    CONSTRAINT whiteboard_tags_whiteboard_id_fkey
        FOREIGN KEY (whiteboard_id) REFERENCES whiteboards(whiteboard_id)
            ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS whiteboard_tags_tag_idx ON whiteboard_tags(whiteboard_tag);

CREATE TABLE IF NOT EXISTS whiteboard_fields
(
    whiteboard_id     varchar(255)  NOT NULL,
    field_name        varchar(255)  NOT NULL,
    field_status      varchar(255)  NOT NULL,
    field_type        text,
    field_type_scheme varchar(255),
    storage_uri       varchar(255),
    finalized_at      timestamp,

    CONSTRAINT whiteboard_fields_pkey PRIMARY KEY (whiteboard_id, field_name),

    CONSTRAINT whiteboard_fields_whiteboard_id_fkey
        FOREIGN KEY (whiteboard_id) REFERENCES whiteboards (whiteboard_id)
            ON DELETE CASCADE
);
