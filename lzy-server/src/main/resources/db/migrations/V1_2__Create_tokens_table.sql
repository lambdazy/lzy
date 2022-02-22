CREATE TABLE tokens
(
    name    text,
    value   text,
    user_id text,
    FOREIGN KEY (user_id) references users (user_id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (name, user_id)
);

ALTER TABLE users
    DROP COLUMN public_token;
