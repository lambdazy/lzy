FROM amazoncorretto:17

RUN apt-get update && apt-get install -y procps
COPY target/allocator-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY cacerts cacerts
RUN cp -f cacerts $JAVA_HOME/lib/security/cacerts

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dmicronaut.env.deduction=true", \
            "-Dio.grpc.netty.shaded.io.netty.eventLoopThreads=10", \
            "-Dio.grpc.netty.level=DEBUG", \
            "-Dsun.net.level=DEBUG", \
            "-jar", \
            "app/app.jar"]
