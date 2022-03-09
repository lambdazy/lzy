#!/bin/bash

MAJOR=1

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <installation-tag> [--rebuild [--base [--update [--major]]]]"
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
CUSTOM_TAG=$1

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -t lzydock/default-env-base -f lzy-servant/docker/DefaultEnv.Base.Dockerfile .
    docker tag lzydock/default-env-base lzydock/default-env-base:master

    docker build -t lzydock/default-env -f lzy-servant/docker/DefaultEnv.Dockerfile .
    docker tag lzydock/default-env lzydock/default-env:from-tar

    docker save -o lzy-servant/docker/default-env-image.tar default-env:from-tar

    docker build -t lzydock/lzy-servant-base -f lzy-servant/docker/System.Base.Dockerfile .
    docker tag lzydock/lzy-servant-base lzydock/lzy-servant-base:master
  fi
  mvn clean install -DskipTests
#  docker build -t lzydock/lzy-backoffice-backend:"$CUSTOM_TAG" lzy-backoffice/Dockerfile
#  docker build -t lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile
fi

for SERVICE in $SERVICES; do
  echo "pushing docker for $SERVICE"
  if [[ $UPDATE = true ]]; then
    MINOR=-1
    for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$SERVICE/tags" -O - | jq -r '.[].name'); do
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
  echo "pushing lzydock/$SERVICE:$TAG"
  docker tag "$SERVICE" "lzydock/$SERVICE:$TAG"
  docker push "lzydock/$SERVICE:$TAG"
  echo ""
done

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG"
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG"