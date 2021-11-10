#!/bin/bash

if [[ $1 == "rebuild" ]]
then
    if [[ $2 == "base" ]]
        then
            docker build -t celdwind/lzy:lzy-servant-base -f lzy-servant/BaseDockerfile .
            docker tag lzy-servant-base celdwind/lzy:lzy-servant-base
            docker push celdwind/lzy:lzy-servant-base
    fi
    mvn clean install -DskipTests
fi

docker tag lzy-server celdwind/lzy:lzy-server
docker tag lzy-servant celdwind/lzy:lzy-servant
docker tag lzy-kharon celdwind/lzy:lzy-kharon

docker push celdwind/lzy:lzy-server
docker push celdwind/lzy:lzy-servant
docker push celdwind/lzy:lzy-kharon
