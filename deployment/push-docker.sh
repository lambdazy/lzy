#!/bin/bash

set -e
MAJOR=1

if [[ $# -lt 1 ]]; then
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

IMAGES="lzy-servant default-env lzy-server lzy-kharon lzy-whiteboard lzy-iam"
if [[ $BASE = true ]]; then
  IMAGES="lzy-servant-base default-env-base $IMAGES"
fi
BRANCH=$(echo "$1" | awk '{print tolower($0)}')
CUSTOM_TAG=$2

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -t lzy-servant-base -f servant/docker/System.Base.Dockerfile .
    SERVANT_BASE = "lzy-servant-base"
    docker build -t default-env-base -f servant/docker/DefaultEnv.Base.Dockerfile .
    DEFAULT_ENV_BASE = "default-env-base"
  else
    SERVANT_BASE = "$(latest-docker-image-on-branch.sh lzy-servant-base $BRANCH)"
    docker pull "$SERVANT_BASE"
    DEFAULT_ENV_BASE = "$(latest-docker-image-on-branch.sh default-env-base $BRANCH)"
    docker pull "$DEFAULT_ENV_BASE"
  fi
  mvn clean install -DskipTests

  SERVANT_BASE_TAG = "$(echo $SERVANT_BASE | awk -F: '{print $2}')"
  docker build --build-arg "SERVANT_BASE_TAG=$SERVANT_BASE_TAG" -t lzy-servant -f servant/docker/System.Dockerfile .

  DEFAULT_ENV_BASE_TAG = "$(echo DEFAULT_ENV_BASE | awk -F: '{print $2}')"
  docker build -t --build-arg "DEFAULT_ENV_BASE_TAG=$DEFAULT_ENV_BASE_TAG" default-env -f servant/docker/DefaultEnv.Dockerfile .

  docker build -t lzy-server -f server/Dockerfile server
  docker build -t lzy-whiteboard -f whiteboard/Dockerfile whiteboard
  docker build -t lzy-kharon -f kharon/Dockerfile kharon
  docker build -t lzy-iam -f iam/Dockerfile iam
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-backend:$CUSTOM_TAG" lzy-backoffice/Dockerfile
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-frontend:$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile
fi

PUSHED_IMAGES=""
NL=$'\n'

for IMAGE in $IMAGES; do
  echo "pushing image for $IMAGE"
  if [[ $UPDATE = true ]]; then
    PREV_NAME = "$(latest-docker-image-on-branch.sh $IMAGE $BRANCH)"
    PREV_TAG = "$(echo PREV_NAME | awk -F: '{print $2}')"
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
  docker push "$NEW_NAME" && ([[ $NEW_NAME == *"base"* ]] || docker image rm "$NEW_NAME")
  echo "::set-output name=${$IMAGE#lzy-}-image::$BRANCH-$NEW_TAG" # for github actions
  echo ""
  PUSHED_IMAGES="$PUSHED_IMAGES${NL}$IMAGE-image = \"$NEW_NAME\""
done
echo "$PUSHED_IMAGES"

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG"
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG"
