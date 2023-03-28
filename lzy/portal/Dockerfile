FROM amazoncorretto:17

COPY target/portal.jar app/app.jar

ENTRYPOINT ["java", \
            "-Xmx3G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-jar", \
            "app/app.jar"]