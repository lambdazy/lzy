#!/bin/bash

java \
-Xmx4G \
-Dsun.jnu.encoding=UTF-8 \
-Dfile.encoding=UTF-8 \
-Dcustom.log.file="$LOG_FILE" \
-Dlog4j.configurationFile=/app/resources/log4j2.yaml \
-XX:+HeapDumpOnOutOfMemoryError \
-agentlib:jdwp=transport=dt_socket,server=y,suspend="$SUSPEND_DOCKER",address=*:"$DEBUG_PORT" \
-jar /app/app.jar \
"$@"