from dataclasses import dataclass
from typing import AsyncIterator, Optional, Tuple, Union, cast

from grpclib.client import Channel

from ai.lzy.v1 import server_pb2
from ai.lzy.v1.workflow.workflow_pb2 import SnapshotStorage
from ai.lzy.v1.workflow.workflow_service_grpc import LzyWorkflowServiceStub
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    AttachWorkflowRequest,
    AttachWorkflowResponse,
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    DeleteWorkflowRequest,
    DeleteWorkflowResponse,
    FinishWorkflowRequest,
    FinishWorkflowResponse,
)
from lzy.api.v2.remote_grpc.model import converter
from lzy.storage.credentials import StorageCredentials


@dataclass
class StorageEndpoint:
    bucket: str
    creds: StorageCredentials


class WorkflowServiceClient:
    def __init__(self, channel: Channel):
        self.stub = LzyWorkflowServiceStub(channel)

    def create_storage_endpoint(
        self,
        response: CreateWorkflowResponse,
    ) -> StorageEndpoint:
        error_msg = "no storage credentials provided"

        assert response.HasField("internalSnapshotStorage"), error_msg
        store: SnapshotStorage = response.internalSnapshotStorage

        grpc_creds: converter.storage_creds.grpc_STORAGE_CREDS
        if store.HasField("azure"):
            grpc_creds = store.azure
        elif store.HasField("amazon"):
            grpc_creds = store.amazon
        else:
            raise ValueError(error_msg)

        creds: StorageCredentials = converter.storage_creds.from_(grpc_creds)
        return StorageEndpoint(store.bucket, creds)

    async def create_workflow(
        self,
        name: str,
    ) -> AsyncIterator[Tuple[str, StorageEndpoint]]:
        async with self.stub.CreateWorkflow.open() as stream:
            await stream.send_request()  # init

            request = CreateWorkflowRequest(workflowName=name)
            await stream.send_message(request)

            async for response in stream:
                yield (
                    response.executionId,
                    self.create_storage_endpoint(response),
                )

    async def attach_workflow(
        self,
        name: str,
        execution_id: str,
    ) -> AttachWorkflowResponse:
        request = AttachWorkflowRequest(
            workflowName=name,
            executionId=execution_id,
        )
        return await self.stub.AttachWorkflow(request)

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
        await self.stub.FinishWorkflow(request)

    async def delete_workflow(self, name: str) -> None:
        request = DeleteWorkflowRequest(workflowName=name)
        await self.stub.DeleteWorkflow(request)
