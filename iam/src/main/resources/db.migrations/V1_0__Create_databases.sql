create TABLE IF NOT EXISTS roles
(
    role text NOT NULL primary key
);

INSERT INTO roles (role)
values ('lzy.workflow.owner'),
       ('lzy.whiteboard.owner')
ON CONFLICT DO NOTHING;

create TABLE IF NOT EXISTS user_resource_roles
(
    user_id  text NOT NULL,
    resource_id text NOT NULL,
    resource_type text NOT NULL,
    role     text NOT NULL,
    primary key (user_id, resource_id, role)
);