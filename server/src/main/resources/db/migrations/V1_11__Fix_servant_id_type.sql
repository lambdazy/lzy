ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_servant_id_fkey;

ALTER TABLE tasks ALTER COLUMN servant_id TYPE text;
ALTER TABLE servants ALTER COLUMN servant_id TYPE text;

ALTER TABLE tasks ADD FOREIGN KEY (servant_id) REFERENCES servants(servant_id);