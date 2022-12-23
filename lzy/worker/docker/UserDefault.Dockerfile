ARG USER_DEFAULT_BASE_TAG
FROM lzydock/user-default-base:${USER_DEFAULT_BASE_TAG}

### copy lzy-py sources & install
COPY docker/tmp-for-context/pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'
