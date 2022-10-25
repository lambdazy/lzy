ARG TEST_ENV_BASE_TAG
FROM lzydock/test-env-base:${TEST_ENV_BASE_TAG}

### copy lzy-py sources & install
COPY docker/tmp-for-context/pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'
