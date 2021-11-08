FROM findepi/graalvm:java11-all

### conda setup
ENV PATH="/root/miniconda3/bin:$PATH"
ARG PATH="/root/miniconda3/bin:$PATH"
RUN apt-get install -y wget
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

SHELL ["/bin/bash", "-c"]

# for future interactive shell sessions
RUN conda init bash

COPY lzy-servant/create_env.sh /
RUN chmod a+rx /create_env.sh
RUN ./create_env.sh "default" "3.9.7" \
    && ./create_env.sh "py37" "3.7.11" \
    && ./create_env.sh "py38" "3.8.12" \
    && ./create_env.sh "py39" "3.9.7" \
    && ./create_env.sh "py310" "3.10.0"

### deps
RUN apt-get -y update --fix-missing
RUN apt-get install -y fuse lsof procps
RUN mkdir -p /var/log/servant

### copy sources
COPY lzy-python/ lzy-python
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar

COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY lzy-servant/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
