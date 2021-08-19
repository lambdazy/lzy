FROM findepi/graalvm:java11-all

COPY target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

RUN apt-get install -y fuse

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", \
            "-jar", \
            "app/app.jar"]