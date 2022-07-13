FROM findepi/graalvm:java17-all

RUN apt-get update && apt-get install -y procps
COPY target/server-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dmicronaut.env.deduction=true", \
            "-Dlzy.server.task.type=kuber", \
            "-Dlzy.servant.pod.template.file=/app/resources/kubernetes/lzy-servant-pod-template.yaml", \
            "-Dservant.lock.pod.template.file=/app/resources/kubernetes/lzy-servant-pod-template.yaml", \
            "-jar", \
            "app/app.jar"]