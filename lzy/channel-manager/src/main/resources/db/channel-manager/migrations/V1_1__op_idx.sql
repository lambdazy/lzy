CREATE UNIQUE INDEX idempotency_key_to_operation_index ON operation (idempotency_key);
CREATE UNIQUE INDEX failed_operations_index ON operation (id) WHERE done = TRUE AND error IS NOT NULL;
CREATE UNIQUE INDEX completed_operations_index ON operation (id) WHERE done = TRUE;
