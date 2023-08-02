CREATE TABLE yc_s3_credentials
(
    user_id         TEXT NOT NULL PRIMARY KEY,
    service_account TEXT NOT NULL,
    access_token    TEXT NOT NULL,
    secret_token    TEXT NOT NULL
);
