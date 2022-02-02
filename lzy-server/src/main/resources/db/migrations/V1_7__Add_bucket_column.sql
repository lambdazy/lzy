ALTER TABLE users
    ADD COLUMN bucket text UNIQUE;

UPDATE users
SET (user_id, bucket) = (SELECT user_id, LOWER(user_id) as bucket);