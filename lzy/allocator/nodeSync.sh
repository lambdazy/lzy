#!/bin/sh

curl -d "{\"cluster_id\":\"$CLUSTER_ID\", \"node_name\":\"$NODE_NAME\"}" -H "Content-Type: application/json" -X POST http://[$1]:8082/kuber_node/set_ready
