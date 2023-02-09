FROM findepi/graalvm:java17-all

RUN apt-get update && apt-get install -y procps netcat net-tools curl tar wget iproute2 iputils-ping
COPY target/tunnel-agent.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dio.grpc.netty.shaded.io.netty.eventLoopThreads=10", \
            "-Dmicronaut.env.deduction=true", \
            "-jar", \
            "app/app.jar"]