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

COPY pylzy /pylzy
WORKDIR /pylzy
RUN pip install wheel setuptools

RUN python setup.py bdist_wheel --dist-dir /dist

FROM continuumio/miniconda3:4.12.0 as conda-build

COPY pylzy/requirements.txt /requirements.txt

RUN conda config --set pip_interop_enabled True
RUN conda init bash

RUN bash -c "eval \"\$(conda shell.bash hook)\" && conda create --name lzy-base && conda activate lzy-base && pip install -r /requirements.txt"

RUN conda create --name py39 --clone lzy-base && conda install -n py39 python=3.9.7
RUN conda create --name py38 --clone lzy-base && conda install -n py38 python=3.8.12
RUN conda create --name py37 --clone lzy-base && conda install -n py37 python=3.7.11

COPY --from=build-pylzy /dist /dist

RUN bash -c "eval \"\$(conda shell.bash hook)\" && conda activate py39 && pip install /dist/*.whl"
RUN bash -c "eval \"\$(conda shell.bash hook)\" && conda activate py38 && pip install /dist/*.whl"
RUN bash -c "eval \"\$(conda shell.bash hook)\" && conda activate py37 && pip install /dist/*.whl"

RUN rm -rf /dist && rm /requirements.txt && conda clean -a

FROM continuumio/miniconda3:4.12.0

COPY --from=build-jre /opt/jre-minimal /opt/jre-minimal
ENV JAVA_HOME=/opt/jre-minimal
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY servant/target/servant-1.0-SNAPSHOT.jar app/app.jar

COPY --from=conda-build /opt/conda/envs /opt/conda/envs

RUN apt-get update && apt-get install -y \
    fuse \
 && rm -rf /var/lib/apt/lists/*


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