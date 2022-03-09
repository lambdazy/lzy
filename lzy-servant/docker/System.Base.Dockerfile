FROM nvidia/cuda:11.4.2-devel-ubuntu20.04

### deps
RUN apt-get -y update && \
    apt-get -y install fuse lsof procps curl bash tar wget \
                       ca-certificates openssh-client iptables && \
    rm -rf /var/lib/apt/lists/*

### dind installation, by https://github.com/cruizba/ubuntu-dind
ENV DOCKER_CHANNEL=stable \
	DOCKER_VERSION=20.10.9 \
	DOCKER_COMPOSE_VERSION=1.29.2 \
	DEBUG=false

COPY lzy-servant/docker/dind/docker_installer.sh /
RUN chmod a+rx /docker_installer.sh
RUN ./docker_installer.sh

COPY lzy-servant/docker/dind/modprobe /usr/local/bin/modprobe
RUN chmod +x /usr/local/bin/modprobe

VOLUME /var/lib/docker

### Docker compose installation
RUN curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
	&& chmod +x /usr/local/bin/docker-compose && docker-compose version

### java
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -y update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

### conda setup
ENV PATH="/root/miniconda3/bin:$PATH"
ARG PATH="/root/miniconda3/bin:$PATH"
RUN apt-get update && \
    apt-get install -y wget
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

SHELL ["/bin/bash", "-c"]

# for future interactive shell sessions
RUN conda init bash

COPY lzy-servant/docker/requirements.txt /
COPY lzy-servant/docker/conda_prepare.sh /
RUN chmod a+rx /conda_prepare.sh
RUN ./conda_prepare.sh init

### cached default-env
# docker save -o default-env-image.tar default-env:from-tar
COPY --chmod=444 lzy-servant/docker/default-env-image.tar /
