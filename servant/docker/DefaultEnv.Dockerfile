FROM lzydock/default-env-base:master

### copy lzy-py sources & install
COPY pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'
