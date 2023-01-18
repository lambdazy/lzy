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

CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
CREATE UNIQUE INDEX failed_operations_index ON operation (id) WHERE done = TRUE AND error IS NOT NULL;

CREATE TABLE session
(
    id                TEXT      NOT NULL PRIMARY KEY,
    owner             TEXT      NOT NULL,
    description       TEXT      NULL,
    cache_policy_json TEXT      NOT NULL,

    created_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    op_id             TEXT      NOT NULL REFERENCES operation (id)
);

CREATE TABLE vm
(
-- spec
    id                    TEXT      NOT NULL PRIMARY KEY,
    session_id            TEXT      NOT NULL,
    pool_label            TEXT      NOT NULL,
    zone                  TEXT      NOT NULL,
    init_workloads_json   TEXT      NOT NULL,
    workloads_json        TEXT      NOT NULL,
    volume_requests_json  TEXT      NOT NULL,
    v6_proxy_address      TEXT      NULL,
    cluster_type          TEXT      NOT NULL,

-- state
    -- overall status
    status                TEXT      NOT NULL, -- INIT, ALLOCATING, RUNNING, IDLE, DELETING

    -- allocation progress
    allocation_op_id      TEXT      NOT NULL REFERENCES operation (id),
    allocation_started_at TIMESTAMP NOT NULL,
    allocation_deadline   TIMESTAMP NOT NULL,
    owner_instance        TEXT      NOT NULL, -- instance_id which serves this operation
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
CREATE INDEX session_vm_index ON vm (session_id);

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
    op_id          TEXT                NOT NULL PRIMARY KEY,
    started_at     TIMESTAMP           NOT NULL,
    deadline       TIMESTAMP           NOT NULL,
    owner_instance TEXT                NOT NULL, -- instance_id which serves this operation

    op_type        disk_operation_type NOT NULL,
    state_json     TEXT                NOT NULL, -- some operation specific state

-- operation failed, rollback required
    failed         BOOLEAN             NOT NULL DEFAULT FALSE,
    fail_reason    TEXT                NULL
);

CREATE TABLE dead_vms
(
    id TEXT      NOT NULL PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    vm JSONB     NOT NULL
);

CREATE TABLE gc_lease
(
    gc         TEXT      NOT NULL PRIMARY KEY,
    owner      TEXT      NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    expired_at TIMESTAMP NOT NULL
);

INSERT INTO gc_lease (gc, owner, updated_at, expired_at)
VALUES ('default', 'none', 'epoch'::TIMESTAMP, 'epoch'::TIMESTAMP);
