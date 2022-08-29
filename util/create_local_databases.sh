#!/bin/bash

su - postgres

createuser lzy_user
psql -c "alter user lzy_user with encrypted password 'q'"
psql -c "alter role lzy_user superuser"

exit 1

dbs=("iam" "allocator" "storage" "kharon" "channel_manager" "scheduler" "graph_executor")

function create_db() {
  echo "Create db $1"
  createdb "lzy_$1_db"
  psql -c "grant all privileges on database lzy_$1_db to lzy_user;"
}

for db in "${dbs[@]}"; do
  create_db $db
done
