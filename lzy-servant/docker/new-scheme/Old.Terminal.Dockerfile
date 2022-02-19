FROM celdwind/lzy:default-env

RUN apt-get -y update
RUN apt-get install -y fuse lsof procps software-properties-common

### java
RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get -y update
RUN apt-get install -y openjdk-11-jdk
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

RUN mkdir -p /var/log/servant

### copy jar & resources
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/docker/terminal-entrypoint.sh /entrypoint.sh
RUN chmod a+rx /entrypoint.sh

COPY lzy-servant/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]