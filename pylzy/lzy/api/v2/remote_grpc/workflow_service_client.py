from dataclasses import dataclass
from typing import Optional, Tuple

from grpclib.client import Channel

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

    async def create_workflow(self, name: str) -> Tuple[str, StorageEndpoint]:
        request = CreateWorkflowRequest(workflowName=name)
        response = await self.stub.CreateWorkflow(request)

        endpoint: Optional[StorageEndpoint] = None
        if response.HasField("internalSnapshotStorage"):
            store = response.internalSnapshotStorage
            creds: Optional[StorageCredentials] = None
            if store.HasField("azure"):
                creds = converter.storage_creds.from_(store.azure)
            if store.HasField("amazon"):
                creds = converter.storage_creds.from_(store.amazon)
            assert creds is not None
            endpoint = StorageEndpoint(store.bucket, creds)

        assert endpoint is not None
        return response.executionId, endpoint

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
