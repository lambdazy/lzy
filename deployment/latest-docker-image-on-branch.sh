#!/bin/bash

set -e
MAJOR=1

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <image-name> <git-branch-name>"
  exit
fi

IMAGE = $1
BRANCH = $(echo "$2" | awk '{print tolower($0)}')

MINOR=-1
for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$IMAGE/tags" -O - | jq -r '.[].name'); do
  if [[ -n $(echo "$TAG" | grep -E "$BRANCH-[0-9]+\.[0-9]+") ]]; then
    VERSION=$(echo "$TAG" | sed "s/$BRANCH-//")
    CUR_MAJOR=$(echo "$VERSION" | awk -F. '{print $1}')
    CUR_MINOR=$(echo "$VERSION" | awk -F. '{print $2}')
    if [[ "$MAJOR" = "$CUR_MAJOR" && "$MINOR" -lt "$CUR_MINOR" ]]; then
      MINOR="$CUR_MINOR"
    fi
  fi
done
LATEST_TAG="lzydock/$IMAGE:$BRANCH-$MAJOR.$MINOR"
echo "$LATEST_TAG"
