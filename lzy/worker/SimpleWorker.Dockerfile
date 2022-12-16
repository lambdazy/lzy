FROM amazoncorretto:17 as build-jre

RUN yum install -y binutils

RUN jlink \
    --module-path "$JAVA_HOME/jmods" \
    --add-modules java.base,java.compiler,java.datatransfer,java.desktop,java.instrument,java.logging,java.management,java.naming,java.rmi,java.scripting,java.security.jgss,java.security.sasl,java.sql,java.transaction.xa,java.xml,jdk.attach,jdk.httpserver,jdk.jdi,jdk.unsupported \
    --verbose \
    --strip-debug \
    --compress 2 \
    --no-header-files \
    --no-man-pages \
    --output /opt/jre-minimal

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

RUN conda clean --all --yes

FROM continuumio/miniconda3:latest

RUN apt-get update && apt-get install -y \
    fuse \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build-jre /opt/jre-minimal /opt/jre-minimal
ENV JAVA_HOME=/opt/jre-minimal
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY --from=conda-build /opt/conda/envs /opt/conda/envs

COPY target/worker-1.0-SNAPSHOT.jar app/app.jar

ENTRYPOINT ["java", \
            "-Xmx4G", \
            "-Dsun.jnu.encoding=UTF-8", \
            "-Dfile.encoding=UTF-8", \
            "-XX:+HeapDumpOnOutOfMemoryError", \
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32", \
            "-Dmicronaut.env.deduction=true", \
            "-Dio.grpc.netty.shaded.io.netty.eventLoopThreads=10", \
            "-Dio.grpc.netty.level=DEBUG", \
            "-Dsun.net.level=DEBUG", \
            "-jar", \
            "app/app.jar"]