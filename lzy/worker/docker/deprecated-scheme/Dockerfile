FROM lzydock/lzy-worker-base:old-scheme

RUN mkdir -p /tmp/lzy-log/worker \
    && mkdir tmp/resources

### copy jar & resources
COPY worker/target/worker-1.0-SNAPSHOT.jar app/app.jar
COPY worker/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

### copy lzy-py sources & install
COPY pylzy/ pylzy
COPY worker/target/worker-1.0-SNAPSHOT.jar pylzy/lzy/lzy-worker.jar
RUN ./conda_prepare.sh pylzy_install 'pylzy'

COPY worker/docker/old-scheme/entrypoint.sh /entrypoint.sh
RUN chmod a+rx /entrypoint.sh

COPY worker/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
