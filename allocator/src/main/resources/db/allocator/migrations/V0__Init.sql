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

CREATE TABLE session
(
    id                TEXT NOT NULL PRIMARY KEY,
    owner             TEXT NOT NULL,
    description       TEXT NULL,
    cache_policy_json TEXT NOT NULL,

    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    op_id             TEXT NOT NULL REFERENCES operation (id),

    deleted_at        TIMESTAMP NULL
);

CREATE INDEX session_activity_index ON session (deleted_at) WHERE (deleted_at IS NOT NULL);

CREATE TABLE vm
(
    id                    TEXT      NOT NULL PRIMARY KEY,
    session_id            TEXT      NOT NULL REFERENCES session (id),
    pool_label            TEXT      NOT NULL,
    zone                  TEXT      NOT NULL,
    status                TEXT      NOT NULL,

    allocation_op_id      TEXT      NOT NULL REFERENCES operation (id),
    allocation_started_at TIMESTAMP NOT NULL,
    workloads_json        TEXT      NOT NULL,
    volume_requests_json  TEXT      NOT NULL,

    vm_subject_id         TEXT,
    last_activity_time    TIMESTAMP NULL,
    deadline              TIMESTAMP NULL,
    allocation_deadline   TIMESTAMP NULL,
    allocator_meta_json   TEXT      NULL,
    vm_meta_json          TEXT      NULL,
    volumes_json          TEXT      NULL,

    v6_proxy_address      TEXT      NULL,
    init_workloads_json   TEXT      NOT NULL DEFAULT '[]'
);

CREATE TABLE disk
(
    id      TEXT    NOT NULL PRIMARY KEY,
    name    TEXT    NOT NULL,
    type    TEXT    NOT NULL,
    size_gb INTEGER NOT NULL,
    zone_id TEXT    NOT NULL,
    user_id TEXT    NOT NULL
);
