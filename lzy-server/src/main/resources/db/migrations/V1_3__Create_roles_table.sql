CREATE TABLE roles
(
    name text,
    PRIMARY KEY (name)
);

CREATE TABLE role_to_user
(
    role_id text,
    user_id text,
    PRIMARY KEY (role_id, user_id),
    FOREIGN KEY (role_id) references roles (name) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (user_id) references users (user_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE permissions
(
    name text PRIMARY KEY
);

CREATE TABLE permission_to_role
(
    role_id       text,
    permission_id text,
    PRIMARY KEY (role_id, permission_id),
    FOREIGN KEY (role_id) references roles (name) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (permission_id) references permissions (name) ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO permissions (name)
VALUES ('backoffice.users.create'),
       ('backoffice.internal.privateApi'),
       ('backoffice.users.delete'),
       ('backoffice.users.list');

INSERT INTO roles (name)
VALUES ('admin'),
       ('backoffice');

INSERT INTO permission_to_role (role_id, permission_id)
VALUES ('admin', 'backoffice.users.create'),
       ('admin', 'backoffice.users.delete'),
       ('admin', 'backoffice.users.list'),
       ('backoffice', 'backoffice.internal.privateApi');