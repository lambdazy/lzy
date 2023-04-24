CREATE TABLE IF NOT EXISTS dynamic_mount(
    id                  TEXT    NOT NULL,
    vm_id               TEXT    NULL        REFERENCES vm(id) ON DELETE SET NULL,
    cluster_id          TEXT    NOT NULL,
    volume_request      JSONB   NOT NULL,
    mount_path          TEXT    NOT NULL,
    mount_name          TEXT    NOT NULL,
    worker_id           TEXT    NOT NULL,
    volume_name         TEXT    NULL,
    volume_claim_name   TEXT    NULL,
    mount_op_id         TEXT    NOT NULL    REFERENCES operation(id),
    unmount_op_id       TEXT    NULL        REFERENCES operation(id),
    state               TEXT    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (vm_id, mount_path),
    UNIQUE (vm_id, mount_name)
);

CREATE INDEX IF NOT EXISTS dynamic_mount_vm_id_idx ON dynamic_mount(cluster_id, volume_claim_name);

ALTER TABLE vm
    ADD COLUMN mount_pod_name TEXT DEFAULT NULL;