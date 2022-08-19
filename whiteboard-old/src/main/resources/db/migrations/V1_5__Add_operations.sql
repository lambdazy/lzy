CREATE TABLE execution (
    execution_id SERIAL PRIMARY KEY,
    snapshot_id text NOT NULL REFERENCES snapshot(snapshot_id) ON DELETE CASCADE,
    name text NOT NULL
);

CREATE TABLE input_arg (
    execution_id INTEGER NOT NULL REFERENCES execution(execution_id),
    entry_id text NOT NULL,
    snapshot_id text NOT NULL,
    name text NOT NULL,
    hash text NOT NULL,
    PRIMARY KEY (execution_id, entry_id, snapshot_id, name),
    FOREIGN KEY (entry_id, snapshot_id) REFERENCES snapshot_entry(entry_id, snapshot_id)
);

CREATE TABLE output_arg (
    execution_id INTEGER NOT NULL REFERENCES execution(execution_id),
    entry_id text NOT NULL,
    snapshot_id text NOT NULL,
    name text NOT NULL,
    PRIMARY KEY (execution_id, entry_id, snapshot_id, name),
    FOREIGN KEY (entry_id, snapshot_id) REFERENCES snapshot_entry(entry_id, snapshot_id)
);