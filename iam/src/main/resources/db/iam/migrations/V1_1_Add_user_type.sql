CREATE type user_type as ENUM (
    'USER',
    'SERVANT'
);

-- TODO may be column name should have other name than the type name
-- TODO may be rename with subject
ALTER TABLE users
    ADD COLUMN user_type user_type;

UPDATE users set user_type = CAST('USER' AS user_type);
