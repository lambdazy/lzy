ALTER TABLE users
     ADD COLUMN access_type text;

UPDATE users SET access_type = 'ACCESS_ALLOWED' WHERE access_type is NULL;