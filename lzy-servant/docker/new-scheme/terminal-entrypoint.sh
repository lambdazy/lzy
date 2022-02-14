#!/bin/bash

export LOGS_APPENDER="${LOGS_APPENDER:-LogFile}"

java \
-Xmx4G \
-Dsun.jnu.encoding=UTF-8 \
-Dfile.encoding=UTF-8 \
-Dcustom.log.file="$LOG_FILE" \
-Djava.library.path=/usr/local/lib \
-Dcustom.log.appender="$LOGS_APPENDER" \
-Dlog4j.configurationFile=/app/resources/log4j2.yaml \
-XX:+HeapDumpOnOutOfMemoryError \
-agentlib:jdwp=transport=dt_socket,server=y,suspend="$SUSPEND_DOCKER",address=*:"$DEBUG_PORT" \
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32 \
-jar /app/app.jar \
"$@"