FROM findepi/graalvm:java11-all

RUN apt-get -y update
RUN apt-get install -y fuse
RUN apt-get install -y python3 python3-pip lsof procps
RUN mkdir -p /var/log/servant

COPY lzy-python/ lzy-python
RUN pip3 install lzy-python/ setuptools

COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]