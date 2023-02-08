CREATE INDEX owner_to_session_index ON session (owner) WHERE delete_op_id IS NULL;
