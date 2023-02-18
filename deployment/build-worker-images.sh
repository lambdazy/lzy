#!/bin/bash -e

set -e

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <docker-registry-prefix> <docker-tag> <stored-worker-base-tag> [--base [--install [--latest]]]"
  exit
fi

DOCKER_REGISTRY=$1
DOCKER_TAG=$2
STORED_WORKER_BASE_TAG=$3

BASE=false
INSTALL=false
LATEST=false

for ARG in "$@"; do
  case "$ARG" in
  --base)
    BASE=true
    ;;
  --install)
    INSTALL=true
    ;;
  --latest)
    LATEST=true
    ;;
  esac
done

cd pylzy/
./scripts/gen_proto.sh
cd ..

if [[ $INSTALL = true ]]; then
  mvn clean install -DskipTests
fi

IMAGES=""

cd lzy/worker/
if [[ $BASE = true ]]; then
  echo "Building image worker-base"
  docker build -t "$DOCKER_REGISTRY/worker-base:$DOCKER_TAG" -t "$DOCKER_REGISTRY/worker-base:latest" \
    -f docker/Worker.Base.Dockerfile .
  WORKER_BASE_TAG=$DOCKER_TAG
  IMAGES="$IMAGES $DOCKER_REGISTRY/worker-base:$DOCKER_TAG"

  echo "Building image user-default-base"
  docker build -t "$DOCKER_REGISTRY/user-default-base:$DOCKER_TAG" \
    -t "$DOCKER_REGISTRY/user-default-base:latest" -f docker/UserDefault.Base.Dockerfile .
  USER_DEFAULT_BASE_TAG=$DOCKER_TAG
  IMAGES="$IMAGES $DOCKER_REGISTRY/user-default-base:$DOCKER_TAG"

  echo "Building image user-test-base"
  docker build -t "$DOCKER_REGISTRY/user-test-base:$DOCKER_TAG" \
    -t "$DOCKER_REGISTRY/user-test-base:latest" -f docker/UserTest.Base.Dockerfile .
  USER_TEST_BASE_TAG=$DOCKER_TAG
  IMAGES="$IMAGES $DOCKER_REGISTRY/user-test-base:$DOCKER_TAG"
  if [[ $LATEST == true ]]; then
    IMAGES="$IMAGES $DOCKER_REGISTRY/worker-base:latest $DOCKER_REGISTRY/user-default-base:latest $DOCKER_REGISTRY/user-test-base:latest"
  fi
else
  if [[ -z "$STORED_WORKER_BASE_TAG" ]]; then
    REMOTE_BASE_TAG="latest"
  else
    REMOTE_BASE_TAG="$STORED_WORKER_BASE_TAG"
  fi
  WORKER_BASE="$DOCKER_REGISTRY/worker-base:$REMOTE_BASE_TAG"
  USER_DEFAULT_BASE="$DOCKER_REGISTRY/user-default-base:$REMOTE_BASE_TAG"
  USER_TEST_BASE="$DOCKER_REGISTRY/user-test-base:$REMOTE_BASE_TAG"

  docker pull "$WORKER_BASE"
  WORKER_BASE_TAG="$(echo $WORKER_BASE | awk -F: '{print $2}')"

  docker pull "$USER_DEFAULT_BASE"
  USER_DEFAULT_BASE_TAG="$(echo $USER_DEFAULT_BASE | awk -F: '{print $2}')"

  docker pull "$USER_TEST_BASE"
  USER_TEST_BASE_TAG="$(echo $USER_TEST_BASE | awk -F: '{print $2}')"
fi

mkdir -p docker/tmp-for-context
cp -R ../../pylzy docker/tmp-for-context/pylzy

echo "Building image worker with base tag $WORKER_BASE_TAG"
docker build --build-arg "REGISTRY=$DOCKER_REGISTRY" --build-arg "WORKER_BASE_TAG=$WORKER_BASE_TAG" \
  -t "$DOCKER_REGISTRY/worker:$DOCKER_TAG" -f docker/Worker.Dockerfile .
IMAGES="$IMAGES $DOCKER_REGISTRY/worker:$DOCKER_TAG"

echo "Building image user-default with base tag $USER_DEFAULT_BASE_TAG"
docker build --build-arg "REGISTRY=$DOCKER_REGISTRY" --build-arg "USER_DEFAULT_BASE_TAG=$USER_DEFAULT_BASE_TAG" \
  -t "$DOCKER_REGISTRY/user-default:$DOCKER_TAG" -f docker/UserDefault.Dockerfile .
IMAGES="$IMAGES $DOCKER_REGISTRY/user-default:$DOCKER_TAG"

echo "Building image user-test with base tag $USER_TEST_BASE_TAG"
docker build --build-arg "REGISTRY=$DOCKER_REGISTRY" --build-arg "USER_TEST_BASE_TAG=$USER_TEST_BASE_TAG" \
  -t "$DOCKER_REGISTRY/user-test:$DOCKER_TAG" -f docker/UserTest.Dockerfile .
IMAGES="$IMAGES $DOCKER_REGISTRY/user-test:$DOCKER_TAG"

rm -rf docker/tmp-for-context
cd ../..

PUSHED_IMAGES=""
NL=$'\n'

for IMAGE in $IMAGES; do
  echo "Pushing image $IMAGE"
  docker push "$IMAGE" && docker image rm "$IMAGE"
  PUSHED_IMAGES="$PUSHED_IMAGES${NL}$IMAGE"
done
echo "$PUSHED_IMAGES"
