FROM cr.yandex/yc-internal/java-base:3.4.0

RUN apt-get update -y && apt-get install -y nano netcat

#COPY cacerts cacerts
#RUN export JAVA_HOME=$JDK_home_path
#RUN cp -f cacerts $JAVA_HOME/lib/security/cacerts

COPY target/allocator.jar app/app.jar

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
