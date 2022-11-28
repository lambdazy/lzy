CREATE TABLE operation
(
    id              TEXT      NOT NULL PRIMARY KEY,
    created_by      TEXT      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    modified_at     TIMESTAMP NOT NULL,
    description     TEXT      NOT NULL,
    done            bool      NOT NULL,

    meta            BYTEA     NULL,
    response        BYTEA     NULL,
    error           BYTEA     NULL,

    idempotency_key TEXT      NULL,
    request_hash    TEXT      NULL,

    CHECK (((idempotency_key IS NOT NULL) AND (request_hash IS NOT NULL)) OR
           ((idempotency_key IS NULL) AND (request_hash IS NULL)))
);

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
CREATE INDEX active_operation_index ON operation (id) WHERE done = FALSE;

CREATE TABLE session
(
    id                TEXT      NOT NULL PRIMARY KEY,
    owner             TEXT      NOT NULL,
    description       TEXT      NULL,
    cache_policy_json TEXT      NOT NULL,

    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    op_id             TEXT      NOT NULL REFERENCES operation (id),

    deleted_at        TIMESTAMP NULL
);

CREATE INDEX session_activity_index ON session (deleted_at) WHERE (deleted_at IS NOT NULL);

CREATE TABLE vm
(
-- spec
    id                    TEXT      NOT NULL PRIMARY KEY,
    session_id            TEXT      NOT NULL REFERENCES session (id),
    pool_label            TEXT      NOT NULL,
    zone                  TEXT      NOT NULL,
    init_workloads_json   TEXT      NOT NULL,
    workloads_json        TEXT      NOT NULL,
    volume_requests_json  TEXT      NOT NULL,
    v6_proxy_address      TEXT      NULL,

-- state
    -- overall status
    status                TEXT      NOT NULL,

    -- allocation progress
    allocation_op_id      TEXT      NOT NULL REFERENCES operation (id),
    allocation_started_at TIMESTAMP NOT NULL,
    allocation_deadline   TIMESTAMP NOT NULL,
    vm_ott                TEXT      NOT NULL,
    vm_subject_id         TEXT      NULL,
    tunnel_pod_name       TEXT      NULL,
    allocator_meta_json   TEXT      NULL,
    volume_claims_json    TEXT      NULL,

    -- vm run state
    vm_meta_json          TEXT      NULL,
    last_activity_time    TIMESTAMP NULL,
    deadline              TIMESTAMP NULL
);

CREATE INDEX allocation_operation_vm_index ON vm (allocation_op_id);

CREATE TABLE disk
(
    id      TEXT    NOT NULL PRIMARY KEY,
    name    TEXT    NOT NULL,
    type    TEXT    NOT NULL,
    size_gb INTEGER NOT NULL,
    zone_id TEXT    NOT NULL,
    user_id TEXT    NOT NULL
);

CREATE TYPE disk_operation_type AS ENUM ('CREATE', 'CLONE', 'DELETE');

CREATE TABLE disk_op
(
    op_id       TEXT                NOT NULL PRIMARY KEY,
    started_at  TIMESTAMP           NOT NULL,
    deadline    TIMESTAMP           NOT NULL,

    op_type     disk_operation_type NOT NULL,
    state_json  TEXT                NOT NULL, -- some operation specific state

-- operation failed, rollback required
    failed      BOOLEAN             NOT NULL DEFAULT FALSE,
    fail_reason TEXT                NULL
);
