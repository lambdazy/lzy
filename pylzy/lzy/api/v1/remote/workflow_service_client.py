import asyncio
import os
from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Optional, Sequence, Tuple, Union

# noinspection PyPackageRequirements
from grpc.aio import Channel

from ai.lzy.v1.common.storage_pb2 import StorageConfig
from ai.lzy.v1.long_running.operation_pb2 import GetOperationRequest
from ai.lzy.v1.long_running.operation_pb2 import Operation
from ai.lzy.v1.long_running.operation_pb2_grpc import LongRunningServiceStub
from ai.lzy.v1.workflow.workflow_pb2 import Graph, VmPoolSpec
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    StartWorkflowRequest,
    StartWorkflowResponse,
    ExecuteGraphRequest,
    ExecuteGraphResponse,
    GraphStatusRequest,
    GraphStatusResponse,
    StopGraphRequest,
    FinishWorkflowRequest,
    FinishWorkflowResponse,
    GetAvailablePoolsRequest,
    GetAvailablePoolsResponse,
    ReadStdSlotsRequest,
    ReadStdSlotsResponse
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceStub
from lzy.api.v1.remote.model import converter
from lzy.api.v1.remote.model.converter.storage_creds import to
from lzy.storage.api import S3Credentials, Storage, StorageCredentials
from lzy.utils.grpc import add_headers_interceptor, build_channel, build_token

KEY_PATH_ENV = "LZY_KEY_PATH"
USER_ENV = "LZY_USER"
ENDPOINT_ENV = "LZY_ENDPOINT"


@dataclass
class Waiting:
    pass


@dataclass
class Executing:
    operations_completed: Sequence[str]
    operations_executing: Sequence[str]
    operations_waiting: Sequence[str]
    message: str


@dataclass
class Completed:
    pass


@dataclass
class Failed:
    description: str


GraphStatus = Union[Waiting, Executing, Completed, Failed]


def _create_storage_endpoint(
        response: StartWorkflowResponse,
) -> Storage:
    error_msg = "no storage credentials provided"

    assert response.HasField("internalSnapshotStorage"), error_msg
    store: StorageConfig = response.internalSnapshotStorage

    grpc_creds: converter.storage_creds.grpc_STORAGE_CREDS
    if store.HasField("azure"):
        grpc_creds = store.azure
    elif store.HasField("s3"):
        grpc_creds = store.s3
    else:
        raise ValueError(error_msg)

    creds: StorageCredentials = converter.storage_creds.from_(grpc_creds)
    return Storage(creds, store.uri)


@dataclass
class StdoutMessage:
    data: str


@dataclass
class StderrMessage:
    data: str


Message = Union[StderrMessage, StdoutMessage]


class WorkflowServiceClient:
    def __init__(self):
        self.__stub = None
        self.__ops_stub = None
        self.__channel = None
        self.__is_started = False

    async def __start(self):
        if self.__is_started:
            return
        self.__is_started = True

        user = os.getenv(USER_ENV)
        key_path = os.getenv(KEY_PATH_ENV)
        if user is None:
            raise ValueError(f"User must be specified by env variable {USER_ENV} or `user` argument")
        if key_path is None:
            raise ValueError(f"Key path must be specified by env variable {KEY_PATH_ENV} or `key_path` argument")

        address = os.getenv(ENDPOINT_ENV, "api.lzy.ai:8899")
        token = build_token(user, key_path)
        interceptors = add_headers_interceptor({"authorization": f"Bearer {token}"})
        self.__channel = build_channel(address, interceptors=interceptors)
        await self.__channel.channel_ready()

        self.__stub = LzyWorkflowServiceStub(self.__channel)
        self.__ops_stub = LongRunningServiceStub(self.__channel)

    async def start_workflow(
            self, name: str, storage: Optional[Storage] = None
    ) -> Tuple[str, Optional[Storage]]:
        await self.__start()

        s: Optional[StorageConfig] = None

        if storage is not None:
            if isinstance(storage.credentials, S3Credentials):
                s = StorageConfig(uri=storage.uri, s3=to(storage.credentials))
            else:
                s = StorageConfig(uri=storage.uri, azure=to(storage.credentials))

        res: StartWorkflowResponse = await self.__stub.StartWorkflow(
            StartWorkflowRequest(workflowName=name, snapshotStorage=s)
        )
        exec_id = res.executionId

        if res.internalSnapshotStorage is not None and res.internalSnapshotStorage.uri != "":
            return exec_id, _create_storage_endpoint(res)

        return exec_id, None

    async def _await_op_done(self, op_id: str) -> FinishWorkflowResponse:
        while True:
            op: Operation = await self.__ops_stub.Get(GetOperationRequest(operation_id=op_id))
            if op is None:
                raise RuntimeError('Cannot wait finish portal operation: operation not found')
            if op.done:
                result = FinishWorkflowResponse()
                op.response.Unpack(result)
                return result
            # sleep 300 ms
            await asyncio.sleep(0.3)

    async def finish_workflow(
            self,
            workflow_name: str,
            execution_id: str,
            reason: str,
    ) -> None:
        await self.__start()
        request = FinishWorkflowRequest(
            workflowName=workflow_name,
            executionId=execution_id,
            reason=reason,
        )
        finish_op: Operation = await self.__stub.FinishWorkflow(request)
        await self._await_op_done(finish_op.id)

    async def read_std_slots(self, execution_id: str) -> AsyncIterator[Message]:
        stream: AsyncIterable[ReadStdSlotsResponse] = self.__stub.ReadStdSlots(
            ReadStdSlotsRequest(executionId=execution_id)
        )

        async for msg in stream:
            if msg.HasField("stderr"):
                for line in msg.stderr.data:
                    yield StderrMessage(line)
            elif msg.HasField("stdout"):
                for line in msg.stdout.data:
                    yield StdoutMessage(line)

    async def execute_graph(self, execution_id: str, graph: Graph) -> str:
        await self.__start()

        res: ExecuteGraphResponse = await self.__stub.ExecuteGraph(
            ExecuteGraphRequest(executionId=execution_id, graph=graph)
        )

        return res.graphId

    async def graph_status(self, execution_id: str, graph_id: str) -> GraphStatus:
        await self.__start()

        res: GraphStatusResponse = await self.__stub.GraphStatus(
            GraphStatusRequest(executionId=execution_id, graphId=graph_id)
        )

        if res.HasField("waiting"):
            return Waiting()

        if res.HasField("completed"):
            return Completed()

        if res.HasField("failed"):
            return Failed(res.failed.description)

        return Executing(
            res.executing.operationsCompleted,
            res.executing.operationsExecuting,
            res.executing.operationsWaiting,
            res.executing.message,
        )

    async def graph_stop(self, execution_id: str, graph_id: str):
        await self.__start()
        await self.__stub.StopGraph(
            StopGraphRequest(executionId=execution_id, graphId=graph_id)
        )

    async def get_pool_specs(self, execution_id: str) -> Sequence[VmPoolSpec]:
        await self.__start()

        pools: GetAvailablePoolsResponse = await self.__stub.GetAvailablePools(
            GetAvailablePoolsRequest(executionId=execution_id)
        )

        return pools.poolSpecs

    async def stop(self):
        await self.__channel.close()
