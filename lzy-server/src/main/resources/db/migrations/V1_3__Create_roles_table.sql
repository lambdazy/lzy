CREATE TABLE roles(
    name text,
    PRIMARY KEY(name)
);

CREATE TABLE role_to_user(
    role_id text,
    user_id text,
    PRIMARY KEY (role_id, user_id),
    FOREIGN KEY (role_id) references roles(name),
    FOREIGN KEY (user_id) references users(user_id)
)