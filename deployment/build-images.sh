#!/bin/bash

set -e
DOCKER_BUILDKIT=1
MAJOR=1

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <git-branch-name> <custom-tag> [--rebuild [--base [--update [--major]]]]"
  exit
fi

BRANCH=$(echo "$1" | awk '{print tolower($0)}')
CUSTOM_TAG=$2

BASE=false

for ARG in "$@"; do
  case "$ARG" in
  --base)
    BASE=true
    ;;
  esac
done

# Can add more images to push here
IMAGES="worker user-default user-test"
if [[ $BASE = true ]]; then
  IMAGES="worker-base user-default-base user-test-base $IMAGES"
fi

cd pylzy/ && ./scripts/gen_proto.sh && cd ..
mvn clean install -DskipTests

cd lzy/worker/
if [[ $BASE = true ]]; then
  echo "Building image worker-base"
  docker build -t worker-base -t lzydock/worker-base:local -f docker/Worker.Base.Dockerfile .
  WORKER_BASE_TAG="local"

  echo "Building image user-default-base"
  docker build -t user-default-base -t lzydock/user-default-base:local -f docker/UserDefault.Base.Dockerfile .
  USER_DEFAULT_BASE_TAG="local"

  echo "Building image user-test-base"
  docker build -t user-test-base -t lzydock/user-test-base:local -f docker/UserTest.Base.Dockerfile .
  USER_TEST_BASE_TAG="local"
else
  WORKER_BASE="$(deployment/latest-docker-image-on-branches.sh worker-base $BRANCH master)"
  docker pull "$WORKER_BASE"
  WORKER_BASE_TAG="$(echo $WORKER_BASE | awk -F: '{print $2}')"

  USER_DEFAULT_BASE="$(deployment/latest-docker-image-on-branches.sh user-default-base $BRANCH master)"
  docker pull "$USER_DEFAULT_BASE"
  USER_DEFAULT_BASE_TAG="$(echo $USER_DEFAULT_BASE | awk -F: '{print $2}')"

  USER_TEST_BASE="$(deployment/latest-docker-image-on-branches.sh user-test-base $BRANCH master)"
  docker pull "$USER_TEST_BASE"
  USER_TEST_BASE_TAG="$(echo $USER_TEST_BASE | awk -F: '{print $2}')"
fi

mkdir -p docker/tmp-for-context
cp -R ../../pylzy docker/tmp-for-context/pylzy

echo "Building image worker with base tag $WORKER_BASE_TAG"
docker build --build-arg "WORKER_BASE_TAG=$WORKER_BASE_TAG" -t worker -f docker/Worker.Dockerfile .

echo "Building image user-default with base tag $USER_DEFAULT_BASE_TAG"
docker build --build-arg "USER_DEFAULT_BASE_TAG=$USER_DEFAULT_BASE_TAG" -t user-default -f docker/UserDefault.Dockerfile .

echo "Building image user-test with base tag $USER_TEST_BASE_TAG"
docker build --build-arg "USER_TEST_BASE_TAG=$USER_TEST_BASE_TAG" -t user-test -f docker/UserTest.Dockerfile .

rm -rf docker/tmp-for-context
cd ../..

# Can add more images to build here

PUSHED_IMAGES=""
NL=$'\n'

for IMAGE in $IMAGES; do
  if [[ -z "$CUSTOM_TAG" ]]; then
    echo "Increment tag for image $IMAGE"

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
  echo "Pushing image $IMAGE: $NEW_NAME"
  docker push "$NEW_NAME" && docker image rm "$NEW_NAME"
  echo "::set-output name=${IMAGE#lzy-}-image::$BRANCH-$NEW_TAG" # for github actions
  echo ""
  PUSHED_IMAGES="$PUSHED_IMAGES${NL}$IMAGE-image = \"$NEW_NAME\""
done
echo "$PUSHED_IMAGES"
