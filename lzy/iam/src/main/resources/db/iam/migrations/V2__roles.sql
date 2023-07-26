INSERT INTO roles (role)
VALUES ('lzy.internal.worker'),
       ('lzy.internal.admin');

ALTER TABLE user_resource_roles
    ADD CONSTRAINT role_fk FOREIGN KEY (role) REFERENCES roles (role);
