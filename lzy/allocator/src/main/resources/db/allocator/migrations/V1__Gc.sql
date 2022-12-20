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
