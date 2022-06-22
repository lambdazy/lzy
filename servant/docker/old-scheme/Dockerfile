FROM lzydock/lzy-servant-base:old-scheme

RUN mkdir -p /tmp/lzy-log/servant \
    && mkdir tmp/resources

### copy jar & resources
COPY servant/target/servant-1.0-SNAPSHOT.jar app/app.jar
COPY servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

### copy lzy-py sources & install
COPY pylzy/ pylzy
COPY servant/target/servant-1.0-SNAPSHOT.jar pylzy/lzy/lzy-servant.jar
RUN ./conda_prepare.sh pylzy_install 'pylzy'

COPY servant/docker/old-scheme/entrypoint.sh /entrypoint.sh
RUN chmod a+rx /entrypoint.sh

COPY servant/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
