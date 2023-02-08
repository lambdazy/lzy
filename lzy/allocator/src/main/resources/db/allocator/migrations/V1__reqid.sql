ALTER TABLE vm
    ADD COLUMN allocation_reqid TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN delete_reqid TEXT NULL;

ALTER TABLE session
    ADD COLUMN delete_reqid TEXT NULL;

ALTER TABLE disk_op
    ADD COLUMN reqid TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN description TEXT NOT NULL DEFAULT '';
