#!/bin/sh

mkdir /webfsd-root
echo true > /webfsd-root/isReady
/usr/local/bin/webfsd -p 8042 -r /webfsd-root

curl -d "{\"cluster_id\":\"$CLUSTER_ID\", \"node_name\":\"$NODE_NAME\"}" -H "Content-Type: application/json" -X POST http://$1:8082/kuber_node/set_ready
