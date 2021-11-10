FROM celdwind/lzy:lzy-servant-base

RUN mkdir -p /var/log/servant

### copy sources
COPY lzy-python/ lzy-python
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar

COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
