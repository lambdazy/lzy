#!/bin/bash

python3 create_node_pool.py --folder-id b1gagf3k6somdujj2a15 --cluster-id cata75qbtbg750blp7cb \
 --subnet-id e9bno1trs3mqauujjtvv --service-account-id ajer523lejs72ad941as \
 --node-pool-name gpupool2 --node-pool-id 123 --node-pool-label s --node-pool-kind CPU --node-pool-tains \
 "[sku=gpu:NoSchedule,sku=gpu:NoExecute]" --platform-id gpu-standard-v2 --cpu-count 8 --min-pool-size 1 \
 --max-pool-size 2 --gpu-count 1 --disc-size 64 --disc-type network-hdd --memory 48
