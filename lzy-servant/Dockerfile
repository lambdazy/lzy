FROM celdwind/lzy:lzy-servant-base

RUN mkdir -p /var/log/servant

### copy jar & resources
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

### copy lzy-py sources & install
COPY lzy-python/ lzy-python
RUN ./conda_prepare.sh pylzy_install 'lzy-python'

COPY lzy-servant/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
