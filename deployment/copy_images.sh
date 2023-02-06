#!/bin/bash

set -e

docker image prune -a -f
echo "Pulling allocator-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/allocator:1.1
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/allocator:1.1 lzydock/allocator:1.1
echo "Pushing allocator-image"
docker push lzydock/allocator:1.1
echo "Pushing allocator-image done"

docker image prune -a -f
echo "Pulling iam-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/iam:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/iam:1.0 lzydock/iam:1.0
echo "Pushing iam-image"
docker push lzydock/iam:1.0
echo "Pushing iam-image done"

docker image prune -a -f
echo "Pulling scheduler-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/scheduler:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/scheduler:1.0 lzydock/scheduler:1.0
echo "Pushing scheduler-image"
docker push lzydock/scheduler:1.0
echo "Pushing scheduler-image done"

docker image prune -a -f
echo "Pulling channel-manager-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/channel-manager:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/channel-manager:1.0 lzydock/channel-manager:1.0
echo "Pushing channel-manager-image"
docker push lzydock/channel-manager:1.0
echo "Pushing channel-manager-image done"

docker image prune -a -f
echo "Pulling worker-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/worker:1.2
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/worker:1.2 lzydock/worker:1.2
echo "Pushing worker-image"
docker push lzydock/worker:1.2
echo "Pushing worker-image done"

docker image prune -a -f
echo "Pulling graph-executor-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/graph-executor:1.1
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/graph-executor:1.1 lzydock/graph-executor:1.1
echo "Pushing graph-executor-image"
docker push lzydock/graph-executor:1.1
echo "Pushing graph-executor-image done"

docker image prune -a -f
echo "Pulling lzy-service-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/lzy-service:1.1
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/lzy-service:1.1 lzydock/lzy-service:1.1
echo "Pushing lzy-service-image"
docker push lzydock/lzy-service:1.1
echo "Pushing lzy-service-image done"

docker image prune -a -f
echo "Pulling portal-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/portal:1.1
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/portal:1.1 lzydock/portal:1.1
echo "Pushing portal-image"
docker push lzydock/portal:1.1
echo "Pushing portal-image done"

docker image prune -a -f
echo "Pulling storage-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/storage:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/storage:1.0 lzydock/storage:1.0
echo "Pushing storage-image"
docker push lzydock/storage:1.0
echo "Pushing storage-image done"

docker image prune -a -f
echo "Pulling whiteboard-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/whiteboard:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/whiteboard:1.0 lzydock/whiteboard:1.0
echo "Pushing whiteboard-image"
docker push lzydock/whiteboard:1.0
echo "Pushing whiteboard-image done"

docker image prune -a -f
echo "Pulling site-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/site:1.1
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/site:1.1 lzydock/site:1.1
echo "Pushing site-image"
docker push lzydock/site:1.1
echo "Pushing site-image done"

docker image prune -a -f
echo "Pulling site-frontend-image"
docker pull cr.yandex/crp2nh9s2lfeqpfgs3c0/site-frontend:1.0
docker tag cr.yandex/crp2nh9s2lfeqpfgs3c0/site-frontend:1.0 lzydock/site-frontend:1.0
echo "Pushing site-frontend-image"
docker push lzydock/site-frontend:1.0
echo "Pushing site-frontend-image done"

docker image prune -a -f