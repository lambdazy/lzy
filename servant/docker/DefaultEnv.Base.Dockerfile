FROM ubuntu:xenial

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

COPY servant/docker/requirements.txt /
COPY servant/docker/conda_prepare.sh /
RUN chmod a+rx /conda_prepare.sh
RUN ./conda_prepare.sh init
