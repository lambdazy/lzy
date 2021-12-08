FROM nvidia/cuda:11.4.2-devel-ubuntu20.04

### deps
RUN apt-get -y update --fix-missing
RUN apt-get install -y fuse lsof procps

### java
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get install -y openjdk-11-jdk
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

RUN mkdir -p /var/log/servant

### copy jar & resources
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]