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

#kubectl delete pod lzy-server-pod
kubectl create -f lzy-server-pod.yaml
kubectl wait --for=condition=Ready pod/lzy-server-pod --timeout=60s

#kubectl create -f lzy-kharon-pod.yaml

LZY_SERVER_HOST=$(kubectl get pod lzy-server-pod -o custom-columns=IP:status.podIP)
echo "$LZY_SERVER_HOST"

#kubectl delete pod terminal-pod
#kubectl create -f lzy-kharon-pod.yaml
#kubectl wait --for=condition=Ready pod/lzy-kharon-pod --timeout=60s
#
#echo "executing task \"echo 44\""
#kubectl exec terminal-pod-2 -- bash -c 'export "ZYGOTE={\"fuze\":\"echo 44\",\"provisioning\":\"not implemented\"}" && /tmp/lzy/sbin/run'

#java -jar -Djava.library.path=/usr/local/lib lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar  "--lzy-address" "http://62.84.119.7:8899" "--lzy-mount" "/tmp/lzy" "--private-key" "/Users/nik-tycoon/.ssh/id_rsa" "--host" "localhost" "--port" "9990" "terminal"

# docker run --privileged -e USER=nick-tycoon -e LOG_FILE="terminal" -e SUSPEND_DOCKER=n -e DEBUG_PORT=5006 lzy-servant "--lzy-address" "http://62.84.119.7:8899" "--lzy-mount" "/tmp/lzy" "--private-key" "/Users/nik-tycoon/.ssh/id_rsa" "--host" "localhost" "--port" "9990" --internal-host localhost terminal

#docker run --name terminal --privileged -e USER=nick-tycoon -e LOG_FILE="terminal" -e SUSPEND_DOCKER=n -e DEBUG_PORT=5006 --mount type=bind,source=/Users/nik-tycoon/.ssh/,target=/Users/nik-tycoon/.ssh/ --mount type=bind,source=/var/log/servant/,target=/var/log/servant/ lzy-servant "--lzy-address" "http://62.84.119.7:8899" "--lzy-mount" "/tmp/lzy" "--private-key" "/Users/nik-tycoon/.ssh/id_rsa" "--host" "localhost" "--port" "9990" --internal-host localhost terminal

cd lzy-python && pip install . && python examples/integration/simple_graph.py
