create TABLE IF NOT EXISTS roles
(
    role varchar(255) NOT NULL primary key
);

insert into roles (role)
values ('lzy.workflow.owner'),
       ('lzy.whiteboard.owner')
ON CONFLICT DO NOTHING;

create TABLE users (
    user_id varchar(255) PRIMARY KEY,

    auth_provider varchar(255),
    provider_user_id varchar(255),

    access_type varchar(255)
);

create TABLE IF NOT EXISTS user_resource_roles
(
    user_id  varchar(255) NOT NULL,
    resource_id varchar(255) NOT NULL,
    resource_type varchar(255) NOT NULL,
    role     varchar(255) NOT NULL,
    primary key (user_id, resource_id, role),
    FOREIGN KEY (user_id) references users (user_id) ON update CASCADE ON delete CASCADE
);

create TABLE credentials (
   name varchar(255),
   value varchar(10485761),
   user_id varchar(255),
   type varchar(255),
   PRIMARY KEY (name, user_id),
   FOREIGN KEY (user_id) references users (user_id) ON update CASCADE ON delete CASCADE
);