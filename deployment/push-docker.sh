#!/bin/bash

if [[ $# -lt 1 || -z $1 ]]; then
  echo "Usage: $0 <installation-tag> [rebuild [base]]"
  exit
fi

INSTALLATION=$1

if [[ $2 == "rebuild" ]]; then
  if [[ $3 == "base" ]]; then
    docker build -t lzydock/lzy-servant-base:"$INSTALLATION" -f lzy-servant/BaseDockerfile .
    docker push lzydock/lzy-servant-base:"$INSTALLATION"
  fi
#    mvn clean install -DskipTests
#    docker build -t lzydock/lzy-backoffice-backend:"$INSTALLATION" lzy-backoffice/Dockerfile
#    docker build -t lzydock/lzy-backoffice-frontend:"$INSTALLATION" lzy-backoffice/frontend/Dockerfile
fi

LAST_ARG=$(echo "$@" | awk '{print $NF}')

if [[ $LAST_ARG == "update-version" ]]; then
  echo "HERE1"
  MAX_TAG=0
  while read TAG; do
    echo "$TAG"
    if [[ "$TAG" =~ [0-9]* && "$MAX_TAG" -lt "$TAG" ]]; then
      MAX_TAG="$TAG"
    fi
  done
  NEW_TAG=$(($MAX_TAG + 1))
  echo "new tag is $NEW_TAG"
fi

exit 0

docker tag lzy-server lzydock/lzy-server:"$INSTALLATION"
docker tag lzy-servant lzydock/lzy-servant:"$INSTALLATION"
docker tag lzy-kharon lzydock/lzy-kharon:"$INSTALLATION"
docker tag lzy-whiteboard lzydock/lzy-whiteboard:"$INSTALLATION"

docker push lzydock/lzy-server:"$INSTALLATION"
docker push lzydock/lzy-servant:"$INSTALLATION"
docker push lzydock/lzy-kharon:"$INSTALLATION"
docker push lzydock/lzy-whiteboard:"$INSTALLATION"
#docker push lzydock/lzy-backoffice-backend:"$INSTALLATION"
#docker push lzydock/lzy-backoffice-frontend:"$INSTALLATION"
