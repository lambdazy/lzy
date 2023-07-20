ALTER TABLE workflows
    ADD COLUMN allocator_session_id       TEXT      NULL,
    ADD COLUMN allocator_session_deadline TIMESTAMP NULL;

ALTER TABLE workflow_executions
    DROP COLUMN allocator_session_id,
    DROP COLUMN portal_vm_address;

CREATE TABLE delete_allocator_session_operations
(
    op_id                TEXT NOT NULL PRIMARY KEY,
    service_instance_id  TEXT NOT NULL, -- instance that created the op
    session_id           TEXT NOT NULL,
    delete_session_op_id TEXT,

    FOREIGN KEY (op_id) REFERENCES operation (id)
);

