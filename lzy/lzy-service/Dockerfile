FROM amazoncorretto:17

RUN yum -y install openssl && yum clean all
COPY target/lzy-service.jar app/app.jar

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-jar", \
            "app/app.jar"]