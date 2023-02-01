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

RUN mamba update --all -y

FROM conda-build

RUN apt-get update && apt-get install -y \
    fuse \
    curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /
RUN ua_version=$(curl -s https://storage.yandexcloud.net/yc-unified-agent/latest-version) bash -c 'curl -s -O https://storage.yandexcloud.net/yc-unified-agent/releases/$ua_version/unified_agent && chmod +x ./unified_agent'

COPY --from=build-jre /java /opt/jre-minimal
ENV JAVA_HOME=/opt/jre-minimal
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY target/worker.jar app/app.jar

COPY docker/log_processor.py /log_processor.py
COPY docker/ua_config.yml /logging.yml

RUN chmod +x /log_processor.py

ENV LOGFILE="/tmp/worker.log"

COPY docker/simple_entrypoint.sh /entrypoint.sh
ENV LZY_CONDA_ENVS_LIST="py37,py38,py39"

ENTRYPOINT ["bash", "/entrypoint.sh"]