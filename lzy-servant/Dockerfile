FROM findepi/graalvm:java11-all
# not great idea ofc, but source and conda activate doesn't work otherwise
SHELL ["/bin/bash", "-c"]

# conda installation
ENV PATH="/root/miniconda3/bin:$PATH"
ARG PATH="/root/miniconda3/bin:$PATH"
RUN apt-get install -y wget
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh
RUN conda --version
RUN conda init bash
RUN conda create --name default
# make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "default", "/bin/bash", "-c"]

### lzy-python
COPY lzy-python/ lzy-python
# to be sure that run from conda env
RUN python3 -m pip --version
RUN python3 -m pip install --default-timeout=100 lzy-python/

COPY lzy-servant/entrypoint.sh /
COPY test.yaml /

### deps
RUN apt-get -y update --fix-missing
RUN apt-get install -y fuse lsof procps
RUN mkdir -p /var/log/servant

### java
COPY lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar app/app.jar
COPY lzy-servant/src/main/resources/ app/resources
RUN chmod -R 700 app/resources

RUN chmod a+rx /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]