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

docker push cr.yandex/crppns4pq490jrka0sth/lzy-server:latest
docker push cr.yandex/crppns4pq490jrka0sth/lzy-servant:latest

#kubectl delete pod lzy-server-pod
kubectl create -f lzy-server-pod.yaml
sleep 2s

LZY_SERVER_HOST=$(kubectl get pod lzy-server-pod -o custom-columns=IP:status.podIP)
echo "$LZY_SERVER_HOST"

#kubectl delete pod terminal-pod
#kubectl create -f terminal-pod.yaml
#sleep 20s
#
#echo "executing task \"echo 44\""
#kubectl exec terminal-pod-2 -- bash -c 'export "ZYGOTE={\"fuze\":\"echo 44\",\"provisioning\":\"not implemented\"}" && /tmp/lzy/sbin/run'
