FROM lzy-servant-base

COPY lzy-python/ lzy-python
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar lzy-python/lzy/lzy-servant.jar
RUN ./conda_prepare.sh pylzy_install 'lzy-python'

COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/docker/new-scheme/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY lzy-servant/docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]