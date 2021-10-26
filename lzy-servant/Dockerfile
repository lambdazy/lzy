FROM findepi/graalvm:java11-all

RUN apt-get -y update --fix-missing
RUN apt-get install -y fuse # wget
RUN apt-get install -y python3 python3-pip lsof procps
RUN mkdir -p /var/log/servanproject struct

COPY lzy-python/ lzy-python
# https://stackoverflow.com/questions/43298872/how-to-solve-readtimeouterror-httpsconnectionpoolhost-pypi-python-org-port
RUN pip3 install --default-timeout=100 future
RUN pip3 install lzy-python/

COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

# commented because it actually builds python interpreter from sources: this is the only way to install
# specific interpreter version.
# uncomment if ready to wait for building
#COPY lzy-servant/pyinstaller.sh /
#RUN chmod a+rx /pyinstaller.sh
#RUN ./pyinstaller.sh "3.9.7" "3.8.9"
#RUN ln /usr/local/bin/python3.9 /usr/bin/python

COPY lzy-servant/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]