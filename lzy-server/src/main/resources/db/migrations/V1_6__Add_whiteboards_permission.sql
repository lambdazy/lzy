INSERT INTO permissions (name)
VALUES ('lzy.whiteboard.all');
INSERT INTO roles (name)
VALUES ('user');
INSERT INTO permission_to_role (role_id, permission_id)
VALUES ('user', 'lzy.whiteboard.all');
INSERT INTO role_to_user (role_id, user_id)
SELECT 'user', users.user_id
FROM users;