CREATE TABLE tokens (
   id serial PRIMARY KEY,
   name text,
   value text,
   user_id text,
   FOREIGN KEY (user_id) references users (user_id),
   UNIQUE (name, user_id)
);

ALTER TABLE users
DROP COLUMN public_token;
