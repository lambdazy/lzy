ALTER TABLE workflow_executions DROM COLUMN portal;
DROP TYPE portal_status;

DROP INDEX expired_workflow_executions_index;

ALTER TABLE workflow_executions DROP COLUMN execution_status;
DROP TYPE execution_status;

DROP TABLE graph_op_state;

CREATE TABLE execution_operations
(
    op_id               TEXT NOT NULL PRIMARY KEY,
    op_type             TEXT NOT NULL,
    service_instance_id TEXT NOT NULL, -- instance that created the op
    execution_id        TEXT NOT NULL,
    state_json          TEXT NOT NULL, -- some operation specific state

    FOREIGN KEY (op_id) REFERENCES operation (id)
);
