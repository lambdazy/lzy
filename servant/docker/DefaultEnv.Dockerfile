ARG DEFAULT_ENV_BASE_TAG
FROM lzydock/default-env-base:${DEFAULT_ENV_BASE_TAG}

### copy lzy-py sources & install
COPY pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'
