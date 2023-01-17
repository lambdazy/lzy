CREATE TABLE IF NOT EXISTS whiteboards (
    whiteboard_id        varchar(255)  NOT NULL,
    whiteboard_name      varchar(255)  NOT NULL,
    user_id              varchar(255)  NOT NULL,
    storage_name         varchar(255)  NOT NULL,
    storage_description  text          NOT NULL,
    storage_uri          text          NOT NULL,
    namespace            varchar(255)  NOT NULL,
    whiteboard_status    varchar(255)  NOT NULL,
    created_at           timestamp     NOT NULL,
    registered_at        timestamp     NOT NULL,

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
    data_scheme       text,

    CONSTRAINT whiteboard_fields_pkey PRIMARY KEY (whiteboard_id, field_name),

    CONSTRAINT whiteboard_fields_whiteboard_id_fkey
        FOREIGN KEY (whiteboard_id) REFERENCES whiteboards (whiteboard_id)
            ON DELETE CASCADE
);

CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    meta            BYTEA     NULL,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    done            bool      NOT NULL,

    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
