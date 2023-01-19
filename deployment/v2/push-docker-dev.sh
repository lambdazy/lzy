#!/bin/bash -e

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <git-branch> <docker-registry-prefix> <docker-images-tag>"
    echo "Requires previously built application and docker logged in the desired repository."
    exit 1
fi

BRANCH=$1
REGISTRY_PREFIX=$2
TAG=$3

IMAGES=""

function build_image {
    IMAGE_NAME=$1
    IMAGE_PATH=$2
    FULL_IMAGE_NAME="$REGISTRY_PREFIX/$IMAGE_NAME:$BRANCH-$TAG"
    echo "Building $FULL_IMAGE_NAME from $IMAGE_PATH"
    docker build -t "$FULL_IMAGE_NAME" "$IMAGE_PATH"
    IMAGES="$IMAGES $FULL_IMAGE_NAME"
}

build_image site-frontend frontend
build_image allocator lzy/allocator
build_image channel-manager lzy/channel-manager
build_image graph-executor lzy/graph-executor
build_image iam lzy/iam
build_image lzy-service lzy/lzy-service
build_image portal lzy/portal
build_image scheduler lzy/scheduler
build_image site-backend lzy/site
build_image storage lzy/storage
build_image whiteboard lzy/whiteboard

for IMAGE in $IMAGES; do
    echo "Pushing $IMAGE"
    docker push "$IMAGE" && docker image rm "$IMAGE"
done