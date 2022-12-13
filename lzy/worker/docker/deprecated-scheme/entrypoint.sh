#!/bin/bash

export LOGS_APPENDER="${LOGS_APPENDER:-LogFile}"

java \
-Xmx4G \
-Dsun.jnu.encoding=UTF-8 \
-Dfile.encoding=UTF-8 \
-Dcustom.log.file="$LOG_FILE" \
-Dcustom.log.worker_id="$LZYTASK" \
-Dcustom.log.appender="$LOGS_APPENDER" \
-Dcustom.log.server="$LOGS_SERVER" \
-Dcustom.log.username="$LOGS_USERNAME" \
-Dcustom.log.password="$LOGS_PASSWORD" \
-XX:+HeapDumpOnOutOfMemoryError \
-agentlib:jdwp=transport=dt_socket,server=y,suspend="$SUSPEND_DOCKER",address=*:"$DEBUG_PORT" \
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32 \
-jar /app/app.jar \
"$@"