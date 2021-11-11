CREATE TABLE backoffice_sessions(
    id uuid PRIMARY KEY,
    uid text null,
    FOREIGN KEY (uid) references users(user_id) ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE users
    ADD COLUMN auth_provider text;

ALTER TABLE users
    ADD COLUMN provider_user_id text;