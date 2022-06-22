FROM findepi/graalvm:java17-all

COPY target/lzy-whiteboard-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-jar", \
            "app/app.jar"]