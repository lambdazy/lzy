FROM nvidia/cuda:11.2.0-devel-ubuntu20.04

### conda setup
ENV PATH="/root/miniconda3/bin:$PATH"
ARG PATH="/root/miniconda3/bin:$PATH"
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get install -y libsndfile1
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

SHELL ["/bin/bash", "-c"]

# for future interactive shell sessions
RUN conda init bash

COPY worker/docker/requirements.txt /
COPY worker/docker/conda_prepare.sh /
RUN chmod a+rx /conda_prepare.sh
RUN ./conda_prepare.sh init

### deps
RUN apt-get -y update --fix-missing
RUN apt-get install -y fuse lsof procps

### java
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get install -y openjdk-17-jdk
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME
