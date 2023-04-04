CREATE TABLE IF NOT EXISTS persistent_volume(
    id              TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    disk_id         TEXT    NOT NULL,
    disk_size_gb    INTEGER NOT NULL,
    cluster_id      TEXT    NOT NULL,
    storage_class   TEXT    NOT NULL,
    access_mode     TEXT    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (name, cluster_id)
);

CREATE TABLE IF NOT EXISTS persistent_volume_claim(
    id              TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    volume_id       TEXT    NOT NULL    REFERENCES persistent_volume(id),
    cluster_id      TEXT    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (name, cluster_id)
);

CREATE TABLE IF NOT EXISTS dynamic_mount(
    id              TEXT    NOT NULL,
    vm_id           TEXT    NOT NULL    REFERENCES vm(id),
    cluster_id      TEXT    NOT NULL,
    volume_spec     TEXT    NOT NULL,
    mount_name      TEXT    NOT NULL,
    volume_claim_id TEXT    NULL        REFERENCES persistent_volume_claim(id),
    mount_op_id     TEXT    NOT NULL    REFERENCES operation(id),
    unmount_op_id   TEXT    NULL        REFERENCES operation(id),
    PRIMARY KEY (id)
);

ALTER TABLE vm
    ADD COLUMN mount_pod_name TEXT DEFAULT NULL;