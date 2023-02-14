ARG REGISTRY=lzydock
ARG WORKER_BASE_TAG
FROM ${REGISTRY}/worker-base:${WORKER_BASE_TAG}

RUN mkdir -p /tmp/lzy-log/worker \
    && mkdir -p /tmp/resources \
    && mkdir -p /tmp/local_modules

COPY docker/tmp-for-context/pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'

COPY target/worker.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENV LZY_CONDA_ENVS_LIST="py37,py38,py39"

ENTRYPOINT ["/entrypoint.sh"]
