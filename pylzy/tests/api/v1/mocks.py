import datetime
import uuid
from typing import List, Callable, Optional, Iterable, BinaryIO, Iterator, Sequence, AsyncIterable

# noinspection PyPackageRequirements
import grpc
from ai.lzy.v1.common.storage_pb2 import StorageConfig, S3Credentials
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard

from lzy.logs.config import get_logger

from ai.lzy.v1.workflow.workflow_service_pb2 import CreateWorkflowRequest, CreateWorkflowResponse, \
    FinishWorkflowRequest, FinishWorkflowResponse, ReadStdSlotsRequest, ReadStdSlotsResponse
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceServicer
from lzy.api.v1 import Runtime, LzyCall, LzyWorkflow
from lzy.api.v1.runtime import ProgressStep
from lzy.storage.api import StorageRegistry, Storage, AsyncStorageClient
from lzy.whiteboards.api import WhiteboardIndexClient

_LOG = get_logger(__name__)


class RuntimeMock(Runtime):
    def __init__(self):
        self.calls: List[LzyCall] = []

    async def start(self, workflow: "LzyWorkflow") -> str:
        return str(uuid.uuid4())

    async def exec(self, calls: List[LzyCall], progress: Callable[[ProgressStep], None]) -> None:
        self.calls = calls

    async def destroy(self) -> None:
        pass


class StorageClientMock(AsyncStorageClient):
    async def size_in_bytes(self, uri: str) -> int:
        pass

    async def read(self, uri: str, dest: BinaryIO, progress: Optional[Callable[[int], None]] = None) -> None:
        pass

    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int], None]] = None):
        pass

    async def blob_exists(self, uri: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass

    async def sign_storage_uri(self, uri: str) -> str:
        pass


class WorkflowServiceMock(LzyWorkflowServiceServicer):
    def __init__(self):
        self.fail = False

    def CreateWorkflow(
        self, request: CreateWorkflowRequest, context: grpc.ServicerContext
    ) -> CreateWorkflowResponse:
        _LOG.info(f"Creating wf {request}")

        if self.fail:
            self.fail = False
            context.abort(grpc.StatusCode.INTERNAL, "some_error")

        return CreateWorkflowResponse(
            executionId="exec_id",
            internalSnapshotStorage=StorageConfig(
                uri="s3://bucket/prefix",
                s3=S3Credentials(endpoint="", accessToken="", secretToken=""),
            ),
        )

    def FinishWorkflow(
        self, request: FinishWorkflowRequest, context: grpc.ServicerContext
    ) -> FinishWorkflowResponse:
        _LOG.info(f"Finishing workflow {request}")

        if self.fail:
            self.fail = False
            context.abort(grpc.StatusCode.INTERNAL, "some_error")

        assert request.workflowName == "some_name"
        assert request.executionId == "exec_id"
        return FinishWorkflowResponse()

    def ReadStdSlots(
        self, request: ReadStdSlotsRequest, context: grpc.ServicerContext
    ) -> Iterator[ReadStdSlotsResponse]:
        _LOG.info(f"Registered listener")

        if self.fail:
            self.fail = False
            context.abort(grpc.StatusCode.INTERNAL, "some_error")

        yield ReadStdSlotsResponse(
            stdout=ReadStdSlotsResponse.Data(data=("Some stdout",))
        )
        yield ReadStdSlotsResponse(
            stderr=ReadStdSlotsResponse.Data(data=("Some stderr",))
        )


class StorageRegistryMock(StorageRegistry):
    def register_storage(self, name: str, storage: Storage, default: bool = False) -> None:
        pass

    def unregister_storage(self, name: str) -> None:
        pass

    def config(self, storage_name: str) -> Optional[Storage]:
        pass

    def default_config(self) -> Optional[Storage]:
        return Storage.azure_blob_storage("", "")

    def default_storage_name(self) -> Optional[str]:
        return "storage_name"

    def client(self, storage_name: str) -> Optional[AsyncStorageClient]:
        pass

    def default_client(self) -> Optional[AsyncStorageClient]:
        return StorageClientMock()

    def available_storages(self) -> Iterable[str]:
        pass


class WhiteboardIndexClientMock(WhiteboardIndexClient):
    async def get(self, id_: str) -> Optional[Whiteboard]:
        pass

    async def query(self,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None) -> AsyncIterable[Whiteboard]:
        pass

    async def register(self, wb: Whiteboard) -> None:
        pass

    async def update(self, wb: Whiteboard):
        pass
