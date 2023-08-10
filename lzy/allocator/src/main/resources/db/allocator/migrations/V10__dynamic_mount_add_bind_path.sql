ALTER TABLE dynamic_mount ADD COLUMN IF NOT EXISTS bind_path TEXT;
UPDATE dynamic_mount SET bind_path = mount_path WHERE bind_path IS NULL;
ALTER TABLE dynamic_mount ALTER COLUMN bind_path SET NOT NULL;

ALTER TABLE dynamic_mount ADD CONSTRAINT dynamic_mount_vm_id_bind_path_key UNIQUE (vm_id, bind_path);