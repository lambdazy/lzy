ALTER TABLE vm
    ADD COLUMN allocation_reqid TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN delete_reqid     TEXT NULL;

ALTER TABLE session
    ADD COLUMN delete_reqid TEXT NULL;

ALTER TABLE disk_op
    ADD COLUMN reqid       TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN description TEXT NOT NULL DEFAULT '';

ALTER TABLE vm
    DROP CONSTRAINT vm_check2;
ALTER TABLE vm
    ADD CONSTRAINT vm_check3 CHECK (
            (status != 'DELETING') OR
            ((delete_op_id IS NOT NULL) AND (delete_worker IS NOT NULL) AND (delete_reqid IS NOT NULL)))

