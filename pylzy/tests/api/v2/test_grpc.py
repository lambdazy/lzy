from lzy.api.v2.remote_grpc.graph_executor_client import (
    GraphExecutorClient,
    prepare_tasks_and_channels,
)
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime


def foo():
    prepare_tasks_and_channels(0, [])
