FROM findepi/graalvm:java11-all

RUN apt-get update && apt-get install -y procps
COPY target/lzy-server-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dtasks.taskType=kuber", \
            "-Dlzy.servant.pod.template.file=/app/resources/kubernetes/lzy-servant-pod-template.yaml", \
            "-jar", \
            "app/app.jar"]