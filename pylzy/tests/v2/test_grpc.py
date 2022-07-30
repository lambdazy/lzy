from lzy.api.v2.grpc.graph_executor_client import (
    GraphExecutorClient,
    prepare_tasks_and_channels,
)
from lzy.api.v2.grpc.portal_client import Portal
from lzy.api.v2.grpc.runtime import GrpcRuntime


def foo():
    prepare_tasks_and_channels(0, [])
