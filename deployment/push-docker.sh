#!/bin/bash

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

SERVICES="lzy-server lzy-servant lzy-kharon lzy-whiteboard"
if [[ $BASE = true ]]; then
  SERVICES="lzy-servant-base $SERVICES"
fi
BRANCH=$(echo "$1" | awk '{print tolower($0)}')
CUSTOM_TAG=$2

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -t lzydock/default-env-base:master -f lzy-servant/docker/DefaultEnv.Base.Dockerfile . || exit 1
    docker build -t lzydock/default-env:from-tar -f lzy-servant/docker/DefaultEnv.Dockerfile . || exit 1
    docker save -o lzy-servant/docker/default-env-image.tar lzydock/default-env:from-tar || exit 1
    docker build -t lzy-servant-base -t lzydock/lzy-servant-base:master -f lzy-servant/docker/System.Base.Dockerfile . || exit 1
  fi
  mvn clean install -DskipTests || exit 1
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-backend:$CUSTOM_TAG" lzy-backoffice/Dockerfile || exit 1
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-frontend:$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile || exit 1
fi

for SERVICE in $SERVICES; do
  echo "pushing docker for $SERVICE"
  if [[ $UPDATE = true ]]; then
    MINOR=-1
    for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$SERVICE/tags" -O - | jq -r '.[].name'); do
      if [[ -n $(echo "$TAG" | grep -E "$BRANCH-[0-9]+\.[0-9]+") ]]; then
        VERSION=$(echo "$TAG" | sed "s/$BRANCH-//")
        CUR_MAJOR=$(echo "$VERSION" | awk -F. '{print $1}')
        CUR_MINOR=$(echo "$VERSION" | awk -F. '{print $2}')
        if [[ "$MAJOR" = "$CUR_MAJOR" && "$MINOR" -lt "$CUR_MINOR" ]]; then
          MINOR="$CUR_MINOR"
        fi
      fi
    done || exit 1
    MINOR=$((MINOR + 1))
    TAG="$MAJOR.$MINOR"
  else
    TAG="$CUSTOM_TAG"
  fi
  NEW_TAG="lzydock/$SERVICE:$BRANCH-$TAG"
  echo "pushing $NEW_TAG"
  docker tag "$SERVICE" "$NEW_TAG" || exit 1
  docker push "$NEW_TAG" || exit 1
  echo ""
done

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG" || exit 1
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG" || exit 1