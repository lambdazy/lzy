FROM findepi/graalvm:java17-all

COPY target/lzy-backoffice-0.1.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources
EXPOSE 8080
EXPOSE 8443
ARG keystore
COPY $keystore app/keystore.jks

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-jar", \
            "app/app.jar"]