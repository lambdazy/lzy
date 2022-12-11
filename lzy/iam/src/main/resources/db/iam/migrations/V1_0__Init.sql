CREATE TABLE IF NOT EXISTS roles (
    role TEXT NOT NULL PRIMARY KEY
);

INSERT INTO roles (role)
VALUES ('lzy.workflow.owner'),
       ('lzy.whiteboard.owner'),
       ('lzy.whiteboard.reader'),
       ('lzy.internal.user')
ON CONFLICT DO NOTHING;

CREATE TABLE users (
    user_id          TEXT PRIMARY KEY,             -- generated user_id
    auth_provider    TEXT NOT NULL,                -- github, ...
    provider_user_id TEXT NOT NULL,                -- github login, ...
    access_type      TEXT NOT NULL,                -- ACCESS_PENDING, ACCESS_ALLOWED, ACCESS_DENIED, ...
    user_type        TEXT NOT NULL,                -- USER, SERVANT, VM, ...
    request_hash     TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_users_provider ON users (provider_user_id, auth_provider);

CREATE TABLE IF NOT EXISTS user_resource_roles (
    user_id       TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    role          TEXT NOT NULL,
    PRIMARY KEY (user_id, resource_id, role),
    FOREIGN KEY (user_id) REFERENCES users (user_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE credentials (
    name       TEXT NOT NULL,
    value      TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    type       TEXT NOT NULL,
    expired_at TIMESTAMP DEFAULT NULL,
    PRIMARY KEY (name, user_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK (type != 'OTT' OR expired_at IS NOT NULL)
);
