CREATE TYPE task_status AS ENUM ('PENDING', 'RUNNING', 'FAILED', 'FINISHED');

CREATE TYPE task_type AS ENUM ('UNMOUNT', 'MOUNT');

CREATE TABLE IF NOT EXISTS task(
    id              BIGSERIAL   NOT NULL,
    name            TEXT        NOT NULL,
    entity_id       TEXT        NOT NULL,
    type            task_type   NOT NULL,
    status          task_status NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    updated_at      TIMESTAMP   NOT NULL,
    metadata        JSONB       NOT NULL,
    operation_id    TEXT,
    worker_id       TEXT,
    lease_till      TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (operation_id) REFERENCES operation(id)
);

CREATE INDEX IF NOT EXISTS task_status_entity_id_idx ON task(status, entity_id, id);
