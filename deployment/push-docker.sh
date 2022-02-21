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
  mvn clean install -DskipTests
#  docker build -t lzydock/lzy-backoffice-backend:"$INSTALLATION" lzy-backoffice/Dockerfile
#  docker build -t lzydock/lzy-backoffice-frontend:"$INSTALLATION" lzy-backoffice/frontend/Dockerfile
fi

LAST_ARG=$(echo "$@" | awk '{print $NF}')
SERVICES="lzy-server lzy-servant lzy-kharon lzy-whiteboard"
for SERVICE in $SERVICES; do
  echo "pushing docker for $SERVICE"
  if [[ $LAST_ARG == "update-version" ]]; then
    MAX_TAG=-1
    for TAG in $(wget -q https://registry.hub.docker.com/v1/repositories/lzydock/$SERVICE/tags -O - | sed -e 's/[][]//g' -e 's/"//g' -e 's/ //g' | tr '}' '\n' | awk -F ":" '{print $3}'); do
      if [[ "$TAG" =~ [0-9]* && "$MAX_TAG" -lt "$TAG" ]]; then
        MAX_TAG="$TAG"
      fi
    done
    NEW_TAG=$((MAX_TAG + 1))
    echo "pushing lzydock/$SERVICE:$NEW_TAG"
    docker tag "$SERVICE" lzydock/$SERVICE:$NEW_TAG
    docker push "lzydock/$SERVICE:$NEW_TAG"
  else
    echo "pushing lzydock/$SERVICE:$INSTALLATION"
    docker tag "$SERVICE" "lzydock/$SERVICE:$INSTALLATION"
    docker push "lzydock/$SERVICE:$INSTALLATION"
  fi
  echo ""
done

#docker push lzydock/lzy-backoffice-backend:"$INSTALLATION"
#docker push lzydock/lzy-backoffice-frontend:"$INSTALLATION"
