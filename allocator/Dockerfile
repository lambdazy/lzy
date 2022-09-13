FROM findepi/graalvm:java17-all

RUN apt-get update && apt-get install -y procps
COPY target/allocator-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dmicronaut.env.deduction=true", \
            "-jar", \
            "app/app.jar"]