ALTER TABLE snapshot
    ADD COLUMN creation_date_UTC timestamp NOT NULL DEFAULT('0001-01-02');

ALTER TABLE snapshot
    ADD COLUMN workflow_name text NOT NULL DEFAULT('default-workflow');

ALTER TABLE snapshot
    ADD COLUMN parent_snapshot_id text;