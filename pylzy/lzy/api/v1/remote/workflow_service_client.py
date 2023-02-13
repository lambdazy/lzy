import asyncio
import atexit
import os
from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Optional, Sequence, Union

# noinspection PyPackageRequirements
from grpc.aio import Channel

from lzy.utils.event_loop import LzyEventLoop

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
    AbortWorkflowRequest,
    GetAvailablePoolsRequest,
    GetAvailablePoolsResponse,
    ReadStdSlotsRequest,
    ReadStdSlotsResponse,
    GetOrCreateDefaultStorageRequest,
    GetOrCreateDefaultStorageResponse,
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceStub
from lzy.api.v1.remote.model import converter
from lzy.api.v1.remote.model.converter.storage_creds import to
from lzy.storage.api import S3Credentials, Storage, StorageCredentials, AzureCredentials
from lzy.utils.grpc import add_headers_interceptor, build_channel, build_token, retry, RetryConfig

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


@dataclass
class StdoutMessage:
    data: str


@dataclass
class StderrMessage:
    data: str


Message = Union[StderrMessage, StdoutMessage]
RETRY_CONFIG = RetryConfig(
    initial_backoff_ms=1000,
    max_retry=120,
    backoff_multiplier=1,
    max_backoff_ms=10000
)
CHANNEL: Optional[Channel] = None


@atexit.register
def __channel_cleanup():
    if CHANNEL:
        # noinspection PyTypeChecker
        LzyEventLoop.run_async(CHANNEL.close())


class WorkflowServiceClient:
    def __init__(self):
        self.__stub = None
        self.__ops_stub = None

    async def __start(self):
        if self.__stub and self.__ops_stub:
            return

        user = os.getenv(USER_ENV)
        key_path = os.getenv(KEY_PATH_ENV)
        if user is None:
            raise ValueError(f"User must be specified by env variable {USER_ENV} or `user` argument")
        if key_path is None:
            raise ValueError(f"Key path must be specified by env variable {KEY_PATH_ENV} or `key_path` argument")

        address = os.getenv(ENDPOINT_ENV, "api.lzy.ai:8899")
        token = build_token(user, key_path)
        interceptors = add_headers_interceptor({"authorization": f"Bearer {token}"})

        global CHANNEL
        if not CHANNEL:
            CHANNEL = build_channel(address, interceptors=interceptors,
                                    service_names=("LzyWorkflowService", "LongRunningService"),
                                    enable_retry=True,
                                    keepalive_ms=1000)

        self.__stub = LzyWorkflowServiceStub(CHANNEL)
        self.__ops_stub = LongRunningServiceStub(CHANNEL)

    @retry(config=RETRY_CONFIG, action_name="starting workflow")
    async def start_workflow(self, name: str, storage: Storage, storage_name: str) -> str:
        await self.__start()

        s: StorageConfig
        if isinstance(storage.credentials, S3Credentials):
            s = StorageConfig(uri=storage.uri, s3=to(storage.credentials))
        elif isinstance(storage.credentials, AzureCredentials):
            s = StorageConfig(uri=storage.uri, azure=to(storage.credentials))
        else:
            raise ValueError(f"Invalid storage credentials type {type(storage.credentials)}")

        res: StartWorkflowResponse = await self.__stub.StartWorkflow(
            StartWorkflowRequest(workflowName=name, snapshotStorage=s, storageName=storage_name)
        )
        exec_id = res.executionId
        return exec_id

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

    @retry(config=RETRY_CONFIG, action_name="finishing workflow")
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

    @retry(config=RETRY_CONFIG, action_name="aborting workflow")
    async def abort_workflow(
        self,
        workflow_name: str,
        execution_id: str,
        reason: str,
    ) -> None:
        await self.__stub.AbortWorkflow(
            AbortWorkflowRequest(workflowName=workflow_name, executionId=execution_id, reason=reason)
        )

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

    @retry(config=RETRY_CONFIG, action_name="starting to execute graph")
    async def execute_graph(self, workflow_name: str, execution_id: str, graph: Graph) -> str:
        await self.__start()

        res: ExecuteGraphResponse = await self.__stub.ExecuteGraph(
            ExecuteGraphRequest(workflowName=workflow_name, executionId=execution_id, graph=graph)
        )

        return res.graphId

    @retry(config=RETRY_CONFIG, action_name="getting graph status")
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

    @retry(config=RETRY_CONFIG, action_name="stopping graph")
    async def graph_stop(self, execution_id: str, graph_id: str):
        await self.__start()
        await self.__stub.StopGraph(
            StopGraphRequest(executionId=execution_id, graphId=graph_id)
        )

    @retry(config=RETRY_CONFIG, action_name="getting vm pools specs")
    async def get_pool_specs(self, execution_id: str) -> Sequence[VmPoolSpec]:
        await self.__start()

        pools: GetAvailablePoolsResponse = await self.__stub.GetAvailablePools(
            GetAvailablePoolsRequest(executionId=execution_id)
        )

        return pools.poolSpecs

    @retry(config=RETRY_CONFIG, action_name="getting default storage")
    async def get_or_create_storage(self) -> Optional[Storage]:
        await self.__start()
        resp: GetOrCreateDefaultStorageResponse = await self.__stub.GetOrCreateDefaultStorage(
            GetOrCreateDefaultStorageRequest())
        if resp.HasField("storage"):
            grpc_creds: converter.storage_creds.grpc_STORAGE_CREDS
            if resp.storage.HasField("azure"):
                grpc_creds = resp.storage.azure
            elif resp.storage.HasField("s3"):
                grpc_creds = resp.storage.s3
            else:
                raise ValueError(f"Invalid storage credentials provided: {resp.storage}")
            creds: StorageCredentials = converter.storage_creds.from_(grpc_creds)
            return Storage(creds, resp.storage.uri)
        return None
