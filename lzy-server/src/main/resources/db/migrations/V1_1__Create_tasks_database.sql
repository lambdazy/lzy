CREATE TABLE tasks
(
    tid      uuid PRIMARY KEY,
    token    text,
    owner_id text,
    FOREIGN KEY (owner_id) references users (user_id)
);