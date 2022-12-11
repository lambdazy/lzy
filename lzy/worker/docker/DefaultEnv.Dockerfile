ARG DEFAULT_ENV_BASE_TAG
FROM lzydock/default-env-base:${DEFAULT_ENV_BASE_TAG}

### copy lzy-py sources & install
COPY docker/tmp-for-context/pylzy/ pylzy
COPY target/worker-1.0-SNAPSHOT.jar pylzy/lzy/lzy-worker.jar
RUN ./conda_prepare.sh pylzy_install 'pylzy'
