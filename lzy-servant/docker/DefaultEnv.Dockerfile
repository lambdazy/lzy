FROM default-env-base

### copy lzy-py sources & install
COPY lzy-python/ lzy-python
RUN ./conda_prepare.sh pylzy_install 'lzy-python'
