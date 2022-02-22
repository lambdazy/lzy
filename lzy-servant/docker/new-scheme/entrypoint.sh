#!/bin/bash

export LOGS_APPENDER="${LOGS_APPENDER:-LogFile}"

dockerd &> var/log/dockerd.log &
sleep 5
mount --make-shared /

docker load -i default-env-image.tar

docker ps && java \
-Xmx4G \
-Dsun.jnu.encoding=UTF-8 \
-Dfile.encoding=UTF-8 \
-Dcustom.log.file="$LOG_FILE" \
-Dcustom.log.servant_id="$LZYTASK" \
-Dcustom.log.appender="$LOGS_APPENDER" \
-Dcustom.log.server="$LOGS_SERVER" \
-Dcustom.log.username="$LOGS_USERNAME" \
-Dcustom.log.password="$LOGS_PASSWORD" \
-Dlog4j.configurationFile=/app/resources/log4j2.yaml \
-XX:+HeapDumpOnOutOfMemoryError \
-agentlib:jdwp=transport=dt_socket,server=y,suspend="$SUSPEND_DOCKER",address=*:"$DEBUG_PORT" \
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32 \
-jar /app/app.jar \
"$@"
