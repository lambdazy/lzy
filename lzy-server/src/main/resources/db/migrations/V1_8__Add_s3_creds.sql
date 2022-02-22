ALTER TABLE users
    ADD COLUMN access_key         text NULL,
    ADD COLUMN secret_key         text NULL,
    ADD COLUMN service_account_id text NULL;