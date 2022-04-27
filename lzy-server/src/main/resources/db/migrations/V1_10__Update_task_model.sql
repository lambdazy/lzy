ALTER TABLE tasks
DROP COLUMN token;

CREATE TABLE servants (
    servant_id uuid PRIMARY KEY,
    token text NOT NULL
);

ALTER TABLE tasks
ADD COLUMN servant_id uuid;

ALTER TABLE tasks
ADD FOREIGN KEY (servant_id) REFERENCES servants(servant_id)
ON DELETE CASCADE
ON UPDATE CASCADE;