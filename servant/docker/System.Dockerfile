FROM lzydock/lzy-servant-base:master

RUN mkdir -p /tmp/lzy-log/servant \
    && mkdir tmp/resources

COPY pylzy/ pylzy
COPY servant/target/servant-1.0-SNAPSHOT.jar pylzy/lzy/lzy-servant.jar
RUN ./conda_prepare.sh pylzy_install 'pylzy'

COPY servant/target/lzy-servant-1.0-SNAPSHOT.jar app/app.jar
COPY servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY servant/docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY servant/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
