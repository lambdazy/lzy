#!/bin/bash

export LOGS_APPENDER="${LOGS_APPENDER:-LogFile}"

dockerd &> var/log/dockerd.log &
# Wait for starting dockerd
sleep 5
mount --make-shared /

docker ps && java \
-Xmx4G \
-Dsun.jnu.encoding=UTF-8 \
-Dfile.encoding=UTF-8 \
-jar /app/app.jar \
"$@"
