#!/bin/bash

set -e
export DOCKER_BUILDKIT=1
MAJOR=1

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <git-branch-name> <installation-tag> [--update]"
  exit
fi
echo "Script run with args: $@"

UPDATE=false

for ARG in "$@"; do
  case "$ARG" in
  --update)
    UPDATE=true
    ;;
  esac
done

BRANCH=$(echo "$1" | awk '{print tolower($0)}')
CUSTOM_TAG=$2

PUSHED_IMAGES=""
NL=$'\n'

echo "building default-env"
docker build -t lzydock/default-env-base:master -f servant/docker/DefaultEnv.Base.Dockerfile .
docker build -t lzydock/default-env:from-tar -f servant/docker/DefaultEnv.Dockerfile .

echo "building lzy-servant-base"
docker save -o servant/docker/default-env-image.tar lzydock/default-env:from-tar
docker build -t lzy-servant-base -t lzydock/lzy-servant-base:master -f servant/docker/System.Base.Dockerfile .

echo "pushing default-env as \"lzydock/default-env:from-tar\""
PUSHED_IMAGES="$PUSHED_IMAGES${NL}lzy-servant-base-image = \"lzydock/default-env:from-tar\""
docker push lzydock/lzy-servant-base:master

echo "removing default-env"
docker image rm default-env-base:master
docker image rm default-env:from-tar
rm -f servant/docker/default-env-image.tar

echo "pushing lzy-servant-base as \"lzydock/lzy-servant-base:master\""
PUSHED_IMAGES="$PUSHED_IMAGES${NL}lzy-servant-base-image = \"lzydock/lzy-servant-base:master\""
docker push lzydock/lzy-servant-base:master

SERVICE = "lzy-servant-base"
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
echo "pushing $SERVICE as \"lzydock/$SERVICE:$NEW_TAG\""
docker push "$NEW_TAG" && docker image rm "$NEW_TAG"
PUSHED_IMAGES="$PUSHED_IMAGES${NL}$SERVICE-image = \"lzydock/$SERVICE:$BRANCH-$TAG\""

echo ""
echo "$PUSHED_IMAGES"
