from dataclasses import dataclass
from typing import Optional, Tuple

from ai.lzy.v1.workflow.workflow_pb2 import SnapshotStorage
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    AttachWorkflowRequest,
    AttachWorkflowResponse,
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    DeleteWorkflowRequest,
    FinishWorkflowRequest,
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceStub
from lzy.api.v2.remote_grpc.model import converter
from lzy.api.v2.remote_grpc.model.converter.storage_creds import to, from_
from lzy.api.v2.remote_grpc.utils import build_channel, add_headers_interceptor
from lzy.api.v2.storage import Credentials
from lzy.storage.credentials import StorageCredentials, AmazonCredentials


@dataclass
class StorageEndpoint:
    bucket: str
    creds: StorageCredentials


def _create_storage_endpoint(
        response: CreateWorkflowResponse,
) -> Credentials:
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
    return Credentials(creds, store.bucket)


class WorkflowServiceClient:
    def __init__(self, address: str, token: str):
        channel = build_channel(address, interceptors=[add_headers_interceptor({"Authorization": token})])
        self.stub = LzyWorkflowServiceStub(channel)

    async def create_workflow(self, name: str, storage: Optional[Credentials] = None) \
            -> Tuple[str, Optional[Credentials]]:
        s: Optional[SnapshotStorage] = None

        if storage is not None:
            if isinstance(storage.storage_credentials, AmazonCredentials):
                s = SnapshotStorage(bucket=storage.bucket, amazon=to(storage.storage_credentials))
            else:
                s = SnapshotStorage(bucket=storage.bucket, azure=to(storage.storage_credentials))

        res = await self.stub.CreateWorkflow(CreateWorkflowRequest(workflowName=name, snapshotStorage=s))
        exec_id = res.executionId

        if res.internalSnapshotStorage is not None:
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
        await self.stub.AttachWorkflow(request)

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
