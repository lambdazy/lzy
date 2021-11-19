#!/bin/bash

run="java
-Xmx4G 
-Dsun.jnu.encoding=UTF-8 
-Dfile.encoding=UTF-8 
-Dcustom.log.file=$LOG_FILE
-Dlog4j.configurationFile=/app/resources/log4j2.yaml 
-XX:+HeapDumpOnOutOfMemoryError
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32 
-jar /app/app.jar
$@"


#-agentlib:jdwp=transport=dt_socket,server=y,suspend="$SUSPEND_DOCKER",address=*:"$DEBUG_PORT"

if $($run);
then
  echo success
else
  while :
  do
    echo "failed"
    sleep 10s
  done
fi
