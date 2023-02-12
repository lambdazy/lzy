import datetime
import sys
import uuid
from typing import List, Callable, Optional, Iterable, BinaryIO, Iterator, Sequence, AsyncIterable, Dict

# noinspection PyPackageRequirements
import grpc
# noinspection PyPackageRequirements
from google.protobuf.any_pb2 import Any
from serialzy.serializers.primitive import PrimitiveSerializer

from ai.lzy.v1.common.storage_pb2 import StorageConfig, S3Credentials
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import RegisterWhiteboardRequest, RegisterWhiteboardResponse, \
    UpdateWhiteboardRequest, UpdateWhiteboardResponse, GetRequest, GetResponse, ListRequest, ListResponse
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceServicer

from lzy.logs.config import get_logger

from ai.lzy.v1.workflow.workflow_service_pb2 import StartWorkflowRequest, StartWorkflowResponse, \
    FinishWorkflowRequest, FinishWorkflowResponse, ReadStdSlotsRequest, ReadStdSlotsResponse, \
    AbortWorkflowRequest, AbortWorkflowResponse, GetStorageCredentialsResponse, GetStorageCredentialsRequest
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceServicer
# noinspection PyUnresolvedReferences
from lzy.api.v1 import Runtime, LzyCall, LzyWorkflow, WorkflowServiceClient
from lzy.api.v1.runtime import ProgressStep
from lzy.py_env.api import PyEnvProvider, PyEnv
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry, Storage, AsyncStorageClient
from lzy.whiteboards.api import WhiteboardIndexClient

from ai.lzy.v1.long_running.operation_pb2 import Operation, GetOperationRequest
from ai.lzy.v1.long_running.operation_pb2_grpc import LongRunningServiceServicer

_LOG = get_logger(__name__)


class RuntimeMock(Runtime):
    def __init__(self):
        self.calls: List[LzyCall] = []

    async def storage(self) -> Optional[Storage]:
        return None

    async def start(self, workflow: "LzyWorkflow") -> str:
        return str(uuid.uuid4())

    async def exec(self, calls: List[LzyCall], progress: Callable[[ProgressStep], None]) -> None:
        self.calls = calls

    async def abort(self) -> None:
        pass

    async def destroy(self) -> None:
        pass


class StorageClientMock(AsyncStorageClient):
    async def copy(self, from_uri: str, to_uri: str) -> None:
        pass

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


class OperationsServiceMock(LongRunningServiceServicer):
    def __init__(self):
        self.ops: Dict[str, Operation] = dict()

    def register_operation(self, op: Operation):
        self.ops[op.id] = op

    def Get(self, request: GetOperationRequest, context: grpc.ServicerContext) -> Operation:
        _LOG.info(f"Get operation {request}")
        op = self.ops[request.operation_id]
        if op is None:
            context.abort(grpc.StatusCode.INTERNAL, "Operation not found")
        return op


class WorkflowServiceMock(LzyWorkflowServiceServicer):
    def __init__(self, op_service: OperationsServiceMock):
        self.fail_on_start = False
        self.created = False
        self.__op_service = op_service

    def StartWorkflow(
        self, request: StartWorkflowRequest, context: grpc.ServicerContext
    ) -> StartWorkflowResponse:
        _LOG.info(f"Creating wf {request}")

        if self.fail_on_start:
            self.fail_on_start = False
            context.abort(grpc.StatusCode.INTERNAL, "some_error")

        self.created = True
        return StartWorkflowResponse(
            executionId="exec_id",
            internalSnapshotStorage=StorageConfig(
                uri="s3://bucket/prefix",
                s3=S3Credentials(endpoint="", accessToken="", secretToken=""),
            ),
        )

    def AbortWorkflow(self, request: AbortWorkflowRequest, context) -> AbortWorkflowResponse:
        _LOG.info(f"Aborting wf {request}")
        return AbortWorkflowResponse()

    def FinishWorkflow(
        self, request: FinishWorkflowRequest, context: grpc.ServicerContext
    ) -> Operation:
        _LOG.info(f"Finishing workflow {request}")

        packed = Any()
        packed.Pack(FinishWorkflowResponse())
        op = Operation(id="operation_id", done=True, response=packed)

        self.__op_service.register_operation(op)

        return op

    def ReadStdSlots(
        self, request: ReadStdSlotsRequest, context: grpc.ServicerContext
    ) -> Iterator[ReadStdSlotsResponse]:
        _LOG.info(f"Registered listener")

        yield ReadStdSlotsResponse(
            stdout=ReadStdSlotsResponse.Data(data=("Some stdout",))
        )
        yield ReadStdSlotsResponse(
            stderr=ReadStdSlotsResponse.Data(data=("Some stderr",))
        )

    def GetStorageCredentials(self, request: GetStorageCredentialsRequest,
                              context: grpc.ServicerContext) -> GetStorageCredentialsResponse:
        return GetStorageCredentialsResponse(storage=StorageConfig(
            uri="s3://bucket/prefix",
            s3=S3Credentials(endpoint="", accessToken="", secretToken=""),
        ))


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


class SerializerRegistryMock(LzySerializerRegistry):
    def __init__(self):
        super().__init__()
        for serializer in list(self._serializer_registry.values()):
            self.unregister_serializer(serializer)


class NotAvailablePrimitiveSerializer(PrimitiveSerializer):
    def available(self) -> bool:
        return False


class NotStablePrimitiveSerializer(PrimitiveSerializer):
    def stable(self) -> bool:
        return False


class WhiteboardIndexServiceMock(LzyWhiteboardServiceServicer):
    def __init__(self):
        self.__whiteboards: Dict[str, Whiteboard] = {}

    def RegisterWhiteboard(self, request: RegisterWhiteboardRequest, context) -> RegisterWhiteboardResponse:
        self.__whiteboards[request.whiteboard.id] = request.whiteboard
        return RegisterWhiteboardResponse()

    def UpdateWhiteboard(self, request: UpdateWhiteboardRequest, context) -> UpdateWhiteboardResponse:
        self.__whiteboards[request.whiteboard.id].MergeFrom(request.whiteboard)
        return UpdateWhiteboardResponse()

    def Get(self, request: GetRequest, context) -> GetResponse:
        whiteboard = self.__whiteboards[request.whiteboardId]
        return GetResponse(whiteboard=whiteboard)

    def List(self, request: ListRequest, context) -> ListResponse:
        whiteboards: List[Whiteboard] = []
        for whiteboard in self.__whiteboards.values():
            name = request.name if request.name else whiteboard.name
            tags = request.tags if request.tags else whiteboard.tags

            if name != whiteboard.name:
                continue
            if not (set(whiteboard.tags) <= set(tags)):
                continue

            from_millis = request.createdTimeBounds.from_.ToMilliseconds()
            if from_millis != 0 and whiteboard.createdAt.ToMilliseconds() < from_millis:
                continue
            to_millis = request.createdTimeBounds.to.ToMilliseconds()
            if to_millis != 0 and whiteboard.createdAt.ToMilliseconds() > to_millis:
                continue

            whiteboards.append(whiteboard)
        return ListResponse(whiteboards=whiteboards)

    def clear_all(self) -> None:
        self.__whiteboards.clear()


class EnvProviderMock(PyEnvProvider):
    def __init__(self, libraries: Optional[Dict[str, str]] = None, local_modules_path: Optional[Sequence[str]] = None):
        self.__libraries = libraries if libraries else {}
        self.__local_modules_path = local_modules_path if local_modules_path else []

    def provide(self, namespace: Dict[str, Any]) -> PyEnv:
        info = sys.version_info
        return PyEnv(f"{info.major}.{info.minor}.{info.micro}", self.__libraries, self.__local_modules_path)
