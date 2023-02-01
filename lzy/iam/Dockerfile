FROM amazoncorretto:17

COPY target/iam.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Dmicronaut.env.deduction=true", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-jar", \
            "app/app.jar"]