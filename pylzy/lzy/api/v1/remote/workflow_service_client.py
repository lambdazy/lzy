from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Optional, Sequence, Tuple, Union

# noinspection PyPackageRequirements
from grpc.aio import Channel

from ai.lzy.v1.common.s3_pb2 import S3Locator
from ai.lzy.v1.workflow.workflow_pb2 import Graph, VmPoolSpec
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    AttachWorkflowRequest,
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    DeleteWorkflowRequest,
    ExecuteGraphRequest,
    ExecuteGraphResponse,
    FinishWorkflowRequest,
    GetAvailablePoolsRequest,
    GetAvailablePoolsResponse,
    GraphStatusRequest,
    GraphStatusResponse,
    ReadStdSlotsRequest,
    ReadStdSlotsResponse,
    StopGraphRequest
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceStub
from lzy.api.v1.remote.model import converter
from lzy.api.v1.remote.model.converter.storage_creds import to
from lzy.utils.grpc import add_headers_interceptor, build_channel
from lzy.storage.api import AmazonCredentials, StorageConfig, StorageCredentials


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
    response: CreateWorkflowResponse,
) -> StorageConfig:
    error_msg = "no storage credentials provided"

    assert response.HasField("internalSnapshotStorage"), error_msg
    store: S3Locator = response.internalSnapshotStorage

    grpc_creds: converter.storage_creds.grpc_STORAGE_CREDS
    if store.HasField("azure"):
        grpc_creds = store.azure
    elif store.HasField("amazon"):
        grpc_creds = store.amazon
    else:
        raise ValueError(error_msg)

    creds: StorageCredentials = converter.storage_creds.from_(grpc_creds)
    return StorageConfig(creds, store.bucket)


@dataclass
class StdoutMessage:
    data: str


@dataclass
class StderrMessage:
    data: str


Message = Union[StderrMessage, StdoutMessage]


class WorkflowServiceClient:
    @staticmethod
    async def create(address: str, token: str) -> "WorkflowServiceClient":
        channel = build_channel(
            address, interceptors=add_headers_interceptor({"authorization": f"Bearer {token}"})
        )
        await channel.channel_ready()
        stub = LzyWorkflowServiceStub(channel)
        return WorkflowServiceClient(stub, channel)

    def __init__(self, stub: LzyWorkflowServiceStub, channel: Channel):
        self.__stub = stub
        self.__channel = channel

    async def create_workflow(
        self, name: str, storage: Optional[StorageConfig] = None
    ) -> Tuple[str, Optional[StorageConfig]]:
        s: Optional[S3Locator] = None

        if storage is not None:
            if isinstance(storage.credentials, AmazonCredentials):
                s = S3Locator(bucket=storage.bucket, amazon=to(storage.credentials))
            else:
                s = S3Locator(bucket=storage.bucket, azure=to(storage.credentials))

        res: CreateWorkflowResponse = await self.__stub.CreateWorkflow(
            CreateWorkflowRequest(workflowName=name, snapshotStorage=s)
        )
        exec_id = res.executionId

        if res.internalSnapshotStorage is not None and res.internalSnapshotStorage.bucket != "":
            return exec_id, _create_storage_endpoint(res)

        return exec_id, None

    async def attach_workflow(
        self,
        name: str,
        execution_id: str,
    ):
        request = AttachWorkflowRequest(
            workflowName=name,
            executionId=execution_id,
        )
        await self.__stub.AttachWorkflow(request)

    async def finish_workflow(
        self,
        workflow_name: str,
        execution_id: str,
        reason: str,
    ) -> None:
        request = FinishWorkflowRequest(
            workflowName=workflow_name,
            executionId=execution_id,
            reason=reason,
        )
        await self.__stub.FinishWorkflow(request)

    async def delete_workflow(self, name: str) -> None:
        request = DeleteWorkflowRequest(workflowName=name)
        await self.__stub.DeleteWorkflow(request)

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
        res: ExecuteGraphResponse = await self.__stub.ExecuteGraph(
            ExecuteGraphRequest(executionId=execution_id, graph=graph)
        )

        return res.graphId

    async def graph_status(self, execution_id: str, graph_id: str) -> GraphStatus:
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
        await self.__stub.StopGraph(
            StopGraphRequest(executionId=execution_id, graphId=graph_id)
        )

    async def get_pool_specs(self, execution_id: str) -> Sequence[VmPoolSpec]:
        pools: GetAvailablePoolsResponse = await self.__stub.GetAvailablePools(
            GetAvailablePoolsRequest(executionId=execution_id)
        )

        return pools.poolSpecs

    async def stop(self):
        await self.__channel.close()
