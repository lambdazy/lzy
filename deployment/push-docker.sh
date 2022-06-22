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
    docker build -t lzydock/default-env-base:master -f servant/docker/DefaultEnv.Base.Dockerfile .
    docker build -t lzydock/default-env:from-tar -f servant/docker/DefaultEnv.Dockerfile .
    docker save -o servant/docker/default-env-image.tar lzydock/default-env:from-tar
    docker build -t lzy-servant-base -t lzydock/lzy-servant-base:master -f servant/docker/System.Base.Dockerfile .
    rm -f servant/docker/default-env-image.tar
  fi
  mvn clean install -DskipTests
  docker build -t lzy-servant -f servant/docker/System.Dockerfile .
  docker build -t lzy-server -f server/Dockerfile server
  docker build -t lzy-whiteboard -f whiteboard/Dockerfile whiteboard
  docker build -t lzy-kharon -f kharon/Dockerfile kharon
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-backend:$CUSTOM_TAG" lzy-backoffice/Dockerfile
#  docker build -t "lzydock/$BRANCH/lzy-backoffice-frontend:$CUSTOM_TAG" lzy-backoffice/frontend/Dockerfile
fi

PUSHED_IMAGES=""
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
    done
    MINOR=$((MINOR + 1))
    TAG="$MAJOR.$MINOR"
  else
    TAG="$CUSTOM_TAG"
  fi
  NEW_TAG="lzydock/$SERVICE:$BRANCH-$TAG"
  docker tag "$SERVICE" "$NEW_TAG" && docker image rm "$SERVICE"
  echo "pushing $NEW_TAG"
  docker push "$NEW_TAG" && docker image rm "$NEW_TAG"
  echo ""
  NL=$'\n'
  PUSHED_IMAGES="$PUSHED_IMAGES${NL}$SERVICE-image = \"lzydock/$SERVICE:$BRANCH-$TAG\""
done
echo "$PUSHED_IMAGES"

#docker push lzydock/lzy-backoffice-backend:"$CUSTOM_TAG"
#docker push lzydock/lzy-backoffice-frontend:"$CUSTOM_TAG"