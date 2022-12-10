#!/bin/bash

set -e
DOCKER_BUILDKIT=1
MAJOR=1

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <git-branch-name> <installation-tag> [--rebuild [--base [--update [--major]]]]"
  exit
fi

BASE=false
REBUILD=false
UPDATE=false

for ARG in "$@"; do
  case "$ARG" in
  --rebuild)
    REBUILD=true
    ;;
  --base)
    BASE=true
    ;;
  --update)
    UPDATE=true
    ;;
  esac
done

IMAGES="lzy-worker default-env test-env lzy-server lzy-kharon lzy-whiteboard lzy-iam"
if [[ $BASE = true ]]; then
  IMAGES="lzy-worker-base default-env-base test-env-base $IMAGES"
fi
BRANCH=$(echo "$1" | awk '{print tolower($0)}')
CUSTOM_TAG=$2

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -t lzy-worker-base -t lzydock/lzy-worker-base:local -f worker/docker/System.Base.Dockerfile worker
    WORKER_BASE_TAG="local"
    docker build -t default-env-base -t lzydock/default-env-base:local -f worker/docker/DefaultEnv.Base.Dockerfile worker
    DEFAULT_ENV_BASE_TAG="local"
    docker build -t test-env-base    -t lzydock/test-env-base:local    -f worker/docker/TestEnv.Base.Dockerfile worker
    TEST_ENV_BASE_TAG="local"
  else
    WORKER_BASE="$(deployment/latest-docker-image-on-branches.sh lzy-worker-base $BRANCH master)"
    docker pull "$WORKER_BASE"
    WORKER_BASE_TAG="$(echo $WORKER_BASE | awk -F: '{print $2}')"

    DEFAULT_ENV_BASE="$(deployment/latest-docker-image-on-branches.sh default-env-base $BRANCH master)"
    docker pull "$DEFAULT_ENV_BASE"
    DEFAULT_ENV_BASE_TAG="$(echo $DEFAULT_ENV_BASE | awk -F: '{print $2}')"

    TEST_ENV_BASE="$(deployment/latest-docker-image-on-branches.sh test-env-base $BRANCH master)"
    docker pull "$TEST_ENV_BASE"
    TEST_ENV_BASE_TAG="$(echo $TEST_ENV_BASE | awk -F: '{print $2}')"
  fi
  cd pylzy/ && ./scripts/gen_proto.sh && cd ..
  mvn clean install -DskipTests

  mkdir -p worker/docker/tmp-for-context
  cp -R pylzy worker/docker/tmp-for-context/pylzy
  docker build --build-arg "WORKER_BASE_TAG=$WORKER_BASE_TAG"         -t lzy-worker -f worker/docker/System.Dockerfile worker
  docker build --build-arg "DEFAULT_ENV_BASE_TAG=$DEFAULT_ENV_BASE_TAG" -t default-env -f worker/docker/DefaultEnv.Dockerfile worker
  docker build --build-arg "TEST_ENV_BASE_TAG=$TEST_ENV_BASE_TAG"       -t test-env    -f worker/docker/TestEnv.Dockerfile worker
  rm -rf worker/docker/tmp-for-context

  docker build -t lzy-server -f server/Dockerfile server
  docker build -t lzy-whiteboard -f whiteboard-old/Dockerfile whiteboard-old
  docker build -t lzy-kharon -f kharon/Dockerfile kharon
  docker build -t lzy-iam -f iam/Dockerfile iam
#  docker build -t lzy-allocator -f allocator/Dockerfile allocator
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-backend:$CUSTOM_TAG" lzy-backoffice/Dockerfile
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-frontend:$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile
fi

PUSHED_IMAGES=""
NL=$'\n'

for IMAGE in $IMAGES; do
  echo "pushing image for $IMAGE"
  if [[ $UPDATE = true ]]; then
    PREV_NAME=$(deployment/latest-docker-image-on-branches.sh $IMAGE $BRANCH 2>&1 | sed "s/^.* //")
    PREV_TAG=$(echo $PREV_NAME | awk -F: '{print $2}')
    VERSION=$(echo "$PREV_TAG" | sed "s/$BRANCH-//")
    PREV_MAJOR=$(echo "$VERSION" | awk -F. '{print $1}')
    PREV_MINOR=$(echo "$VERSION" | awk -F. '{print $2}')
    MINOR=-1
    if [[ "$MAJOR" = "$PREV_MAJOR" && "$MINOR" -lt "$PREV_MINOR" ]]; then
      MINOR="$PREV_MINOR"
    fi
    MINOR=$((MINOR + 1))
    NEW_TAG="$MAJOR.$MINOR"
  else
    NEW_TAG="$CUSTOM_TAG"
  fi
  NEW_NAME="lzydock/$IMAGE:$BRANCH-$NEW_TAG"
  docker tag "$IMAGE" "$NEW_NAME" && docker image rm "$IMAGE"
  echo "pushing $NEW_NAME"
  docker push "$NEW_NAME" && docker image rm "$NEW_NAME"
  echo "::set-output name=${IMAGE#lzy-}-image::$BRANCH-$NEW_TAG" # for github actions
  echo ""
  PUSHED_IMAGES="$PUSHED_IMAGES${NL}$IMAGE-image = \"$NEW_NAME\""
done
echo "$PUSHED_IMAGES"

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG"
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG"
