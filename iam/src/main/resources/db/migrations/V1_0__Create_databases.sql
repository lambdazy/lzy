create TABLE IF NOT EXISTS roles
(
    role text NOT NULL primary key
);

insert into roles (role)
values ('lzy.workflow.owner'),
       ('lzy.whiteboard.owner')
ON CONFLICT DO NOTHING;

create TABLE users (
    user_id text PRIMARY KEY,

    auth_provider text,
    provider_user_id text,

    access_type text
);

create TABLE IF NOT EXISTS user_resource_roles
(
    user_id  text NOT NULL,
    resource_id text NOT NULL,
    resource_type text NOT NULL,
    role     text NOT NULL,
    primary key (user_id, resource_id, role),
    FOREIGN KEY (user_id) references users (user_id) ON update CASCADE ON delete CASCADE
);

create TABLE credentials (
   name text,
   value text,
   user_id text,
   type text,
   PRIMARY KEY (name, user_id),
   FOREIGN KEY (user_id) references users (user_id) ON update CASCADE ON delete CASCADE
);