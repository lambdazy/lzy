ARG SERVANT_BASE_TAG
FROM lzydock/lzy-servant-base:${SERVANT_BASE_TAG}

RUN mkdir -p /tmp/lzy-log/servant \
    && mkdir -p /tmp/resources

COPY docker/tmp-for-context/pylzy/ pylzy
COPY target/servant-1.0-SNAPSHOT.jar pylzy/lzy/lzy-servant.jar
RUN ./conda_prepare.sh pylzy_install 'pylzy'

COPY target/servant-1.0-SNAPSHOT.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
