INSERT INTO permissions (name) VALUES ('lzy.whiteboard.all');
INSERT INTO roles (name) VALUES ('whiteboard.user');
INSERT INTO permission_to_role (role_id, permission_id) VALUES ('whiteboard.user', 'lzy.whiteboard.all');