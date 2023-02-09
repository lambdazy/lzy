FROM amazoncorretto:17

RUN yum -y install openssl && yum clean all

ARG keystore=keystore.jks
COPY $keystore app/keystore.jks

COPY target/site.jar app/app.jar

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-jar", \
            "app/app.jar"]