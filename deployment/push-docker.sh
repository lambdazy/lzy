#!/bin/bash

if [[ $# -lt 1 || -z $1 ]]; then
  echo "Usage: $0 <installation-tag> [--rebuild [--base [--update]]]"
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
INSTALLATION=$1

if [[ $REBUILD = true ]]; then
  if [[ $BASE = true ]]; then
    docker build -f lzy-servant/BaseDockerfile .
  fi
  mvn clean install -DskipTests
#  docker build -t lzydock/lzy-backoffice-backend:"$INSTALLATION" lzy-backoffice/Dockerfile
#  docker build -t lzydock/lzy-backoffice-frontend:"$INSTALLATION" lzy-backoffice/frontend/Dockerfile
fi

for SERVICE in $SERVICES; do
  echo "pushing docker for $SERVICE"
  if [[ $UPDATE = true ]]; then
    MAX_TAG=-1
    for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$SERVICE/tags" -O - | jq -r '.[].name'); do
      if [[ "$TAG" =~ [0-9]* && "$MAX_TAG" -lt "$TAG" ]]; then
        MAX_TAG="$TAG"
      fi
    done
    NEW_TAG=$((MAX_TAG + 1))
    TAG="$NEW_TAG"
  else
    TAG="$INSTALLATION"
  fi
  echo "pushing lzydock/$SERVICE:$TAG"
  docker tag "$SERVICE" "lzydock/$SERVICE:$TAG"
  docker push "lzydock/$SERVICE:$TAG"
  echo ""
done

#docker push lzydock/lzy-backoffice-backend:"$INSTALLATION"
#docker push lzydock/lzy-backoffice-frontend:"$INSTALLATION"
