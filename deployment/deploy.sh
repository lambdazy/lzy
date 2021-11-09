#!/bin/bash

PODS=$(kubectl get pods | sed 1d | awk '{printf $1 " "}')
printf "deleting pods: %s\n" "$PODS"
for POD in $PODS
do
  kubectl delete POD "$POD"
done

mvn clean install -DskipTests

docker tag lzy-server cr.yandex/crppns4pq490jrka0sth/lzy-server:latest
docker tag lzy-servant cr.yandex/crppns4pq490jrka0sth/lzy-servant:latest
docker tag lzy-kharon cr.yandex/crppns4pq490jrka0sth/lzy-kharon:latest

docker push cr.yandex/crppns4pq490jrka0sth/lzy-server:latest
docker push cr.yandex/crppns4pq490jrka0sth/lzy-servant:latest
docker push cr.yandex/crppns4pq490jrka0sth/lzy-kharon:latest

#docker run --name terminal --privileged -e USER=nick-tycoon -e LOG_FILE="terminal" -e SUSPEND_DOCKER=n -e DEBUG_PORT=5006 --mount type=bind,source=/Users/nik-tycoon/.ssh/,target=/Users/nik-tycoon/.ssh/ --mount type=bind,source=/var/log/servant/,target=/var/log/servant/ lzy-servant "--lzy-address" "http://62.84.119.7:8899" "--lzy-mount" "/tmp/lzy" "--private-key" "/Users/nik-tycoon/.ssh/id_rsa" "--host" "localhost" "--port" "9990" --internal-host localhost terminal
