-- TODO may be column name should have other name than the type name
-- TODO may be rename with subject
ALTER TABLE users
    ADD COLUMN user_type user_type VARCHAR(255) NOT NULL DEFAULT 'USER';
