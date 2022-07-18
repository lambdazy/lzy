ARG SERVANT_BASE_TAG
FROM lzydock/lzy-servant-base:${SERVANT_BASE_TAG}

RUN mkdir -p /tmp/lzy-log/servant \
    && mkdir -p /tmp/resources

COPY servant/target/servant-1.0-SNAPSHOT.jar app/app.jar
COPY servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY servant/docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY servant/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
