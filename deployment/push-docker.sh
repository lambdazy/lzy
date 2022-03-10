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
BRANCH=$1
CUSTOM_TAG=$2

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -t lzy-servant-base -t "lzydock/lzy-servant-base:master" -f lzy-servant/BaseDockerfile .
  fi
  mvn clean install -DskipTests
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-backend:$CUSTOM_TAG" lzy-backoffice/Dockerfile
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-frontend:$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile
fi

for SERVICE in $SERVICES; do
  echo "pushing docker for $SERVICE"
  if [[ $UPDATE = true ]]; then
    MINOR=-1
    for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$BRANCH/$SERVICE/tags" -O - | jq -r '.[].name'); do
      if [[ "$TAG" =~ [0-9]+\.[0-9]+ ]]; then
        CUR_MAJOR=$(echo "$TAG" | awk -F. '{print $1}')
        CUR_MINOR=$(echo "$TAG" | awk -F. '{print $2}')
        if [[ "$MAJOR" = "$CUR_MAJOR" && "$MINOR" -lt "$CUR_MINOR" ]]; then
          MINOR="$CUR_MINOR"
        fi
      fi
    done
    MINOR=$((MINOR + 1))
    TAG="$MAJOR.$MINOR"
  else
    TAG="$CUSTOM_TAG"
  fi
  echo "pushing lzydock/$BRANCH/$SERVICE:$TAG"
  docker tag "$SERVICE" "lzydock/$BRANCH/$SERVICE:$TAG"
  docker push "lzydock/$BRANCH/$SERVICE:$TAG"
  echo ""
done

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG"
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG"