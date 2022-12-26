FROM amazoncorretto:17 as build-jre
RUN cp -R $JAVA_HOME /java

FROM python:3.7 as build-pylzy

COPY docker/tmp-for-context/pylzy /pylzy
WORKDIR /pylzy
RUN pip install wheel setuptools

RUN python setup.py bdist_wheel --dist-dir /dist

FROM condaforge/mambaforge:latest as conda-build

SHELL ["/bin/bash", "-l", "-c"]
RUN conda config --set pip_interop_enabled True
RUN conda init bash

COPY docker/base-conda.yaml /env.yaml

RUN mamba env create -n py39 --file=/env.yaml python=3.9.7 && \
    mamba env create -n py38 --file=/env.yaml python=3.8.12 &&  \
    mamba env create -n py37 --file=/env.yaml python=3.7.10

COPY --from=build-pylzy /dist /dist

RUN eval "$(conda shell.bash hook)" &&  \
    conda activate py39 && pip install /dist/*.whl && pip cache purge && \
    conda activate py38 && pip install /dist/*.whl && pip cache purge && \
    conda activate py37 && pip install /dist/*.whl && pip cache purge

FROM condaforge/miniforge3:latest

RUN apt-get update && apt-get install -y \
    fuse \
    curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /
RUN ua_version=$(curl -s https://storage.yandexcloud.net/yc-unified-agent/latest-version) bash -c 'curl -s -O https://storage.yandexcloud.net/yc-unified-agent/releases/$ua_version/unified_agent && chmod +x ./unified_agent'

COPY --from=build-jre /java /opt/jre-minimal
ENV JAVA_HOME=/opt/jre-minimal
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY --from=conda-build /opt/conda/envs /opt/conda/envs
COPY --from=conda-build /opt/conda/conda-meta /opt/conda/conda-meta

# Running conda install to update metadata
RUN conda install -n py39 stdlib-list=0.8.0=pyhd8ed1ab_0  \
    && conda install -n py38 stdlib-list=0.8.0=pyhd8ed1ab_0  \
    && conda install -n py37 stdlib-list=0.8.0=pyhd8ed1ab_0

COPY target/worker-1.0-SNAPSHOT.jar app/app.jar

COPY docker/log_processor.py /log_processor.py
COPY docker/ua_config.yml /logging.yml

RUN chmod +x /log_processor.py

ENV LOGFILE="/tmp/worker.log"

RUN echo '#!/bin/bash\n\
set -m\n\
java -Xmx4G -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 -XX:+HeapDumpOnOutOfMemoryError -Djava.util.concurrent.ForkJoinPool.common.parallelism=32 \
-Dmicronaut.env.deduction=true -Dio.grpc.netty.shaded.io.netty.eventLoopThreads=10 -Dio.grpc.netty.level=DEBUG -Dsun.net.level=DEBUG \
-jar /app/app.jar $@ & \n\
\n\
/unified_agent --config /logging.yml & \n\
fg %1 \n\
sleep 100h' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]