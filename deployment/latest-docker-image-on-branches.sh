#!/bin/bash

set -e
MAJOR=1

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <image-name> <git-branch-name> [<alternative-git-branch-name>]"
  exit
fi

IMAGE=$1
BRANCH=$(echo "$2" | awk '{print tolower($0)}')
if [[ $# -gt 2 ]]; then
  ALT_BRANCH=$(echo "$3" | awk '{print tolower($0)}')
fi

function latest-branch-image-tag {
  br=$1
  MINOR=-1
  for TAG in $(wget -q "https://registry.hub.docker.com/v1/repositories/lzydock/$IMAGE/tags" -O - | jq -r '.[].name'); do
    if [[ -n $(echo "$TAG" | grep -E "$br-[0-9]+\.[0-9]+") ]]; then
      VERSION=$(echo "$TAG" | sed "s/$br-//")
      CUR_MAJOR=$(echo "$VERSION" | awk -F. '{print $1}')
      CUR_MINOR=$(echo "$VERSION" | awk -F. '{print $2}')
      if [[ "$MAJOR" = "$CUR_MAJOR" && "$MINOR" -lt "$CUR_MINOR" ]]; then
        MINOR="$CUR_MINOR"
      fi
    fi
  done
  echo "lzydock/$IMAGE:$br-$MAJOR.$MINOR"
}

LATEST_TAG=$(latest-branch-image-tag $BRANCH)
if [[ $LATEST_TAG =~ \.-1$ ]]; then
  LATEST_TAG=$(latest-branch-image-tag $ALT_BRANCH)
fi

if [[ $LATEST_TAG =~ \.-1$ ]]; then
  echo "Invalid tag $LATEST_TAG"
  exit
fi

echo "$LATEST_TAG"
