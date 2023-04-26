FROM amazoncorretto:17

COPY target/s3-sink.jar app/app.jar

ENTRYPOINT ["java", \
            "-Xmx1g", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Dmicronaut.env.deduction=true", \
            "-Dio.grpc.netty.shaded.io.netty.eventLoopThreads=10", \
            "-jar", \
            "app/app.jar"]