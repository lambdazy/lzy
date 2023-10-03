ARG REGISTRY=lzydock
ARG WORKER_BASE_TAG
FROM ${REGISTRY}/worker-base:${WORKER_BASE_TAG}

RUN mkdir -p /tmp/lzy-log/worker \
    && mkdir -p /tmp/resources \
    && mkdir -p /tmp/local_modules

COPY docker/tmp-for-context/pylzy/ pylzy
RUN ./conda_prepare.sh pylzy_install 'pylzy'

# Creating conda decriptions for every env
RUN eval "$(conda shell.bash hook)" \
    && conda env list --json  \
    | jq '.envs|.[]'  \
    | xargs -I'@' bash -c 'eval "$(conda shell.bash hook)" && conda activate @ && conda env export > @/conda-desc.yaml'

COPY target/worker.jar app/app.jar
COPY src/main/resources/ app/resources
RUN chmod -R 700 app/resources

COPY docker/entrypoint.sh /
RUN chmod a+rx /entrypoint.sh

COPY docker/test_entrypoint.sh /
RUN chmod a+rx /test_entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
