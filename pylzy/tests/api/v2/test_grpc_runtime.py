import asyncio
import dataclasses
import logging
import tempfile
from concurrent import futures
from io import BytesIO
from typing import Iterator
from unittest import TestCase

import aioboto3
import grpc.aio
import requests
from Crypto.PublicKey import RSA
from lzy.api.v1.local.runtime import LocalRuntime

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from grpc import StatusCode
from moto.server import ThreadedMotoServer

from ai.lzy.v1.common.s3_pb2 import AmazonS3Endpoint, S3Locator
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, WhiteboardField, WhiteboardFieldInfo, Storage
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    FinishWorkflowRequest,
    FinishWorkflowResponse,
    ReadStdSlotsRequest,
    ReadStdSlotsResponse,
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    LzyWorkflowServiceServicer,
    add_LzyWorkflowServiceServicer_to_server,
)
from lzy.api.v1 import Lzy, op, whiteboard
from lzy.utils.event_loop import LzyEventLoop
from lzy.api.v1.remote_grpc.runtime import GrpcRuntime
from lzy.whiteboards.whiteboard import _ReadOnlyWhiteboard
from lzy.api.v1.snapshot import DefaultSnapshot
import lzy.api.v1.startup as startup
from lzy.api.v1.utils._pickle import pickle
from lzy.proxy.result import Just
from lzy.serialization.registry import DefaultSerializerRegistry
from lzy.serialization.types import File
from lzy.storage import api as storage
from lzy.storage.registry import DefaultStorageRegistry

logging.basicConfig(level=logging.DEBUG)

LOG = logging.getLogger(__name__)


class WorkflowServiceMock(LzyWorkflowServiceServicer):
    def __init__(self):
        self.fail = False

    def CreateWorkflow(
        self, request: CreateWorkflowRequest, context: grpc.ServicerContext
    ) -> CreateWorkflowResponse:
        LOG.info(f"Creating wf {request}")

        if self.fail:
            self.fail = False
            context.abort(StatusCode.INTERNAL, "some_error")

        return CreateWorkflowResponse(
            executionId="exec_id",
            internalSnapshotStorage=S3Locator(
                bucket="",
                amazon=AmazonS3Endpoint(endpoint="", accessToken="", secretToken=""),
            ),
        )

    def FinishWorkflow(
        self, request: FinishWorkflowRequest, context: grpc.ServicerContext
    ) -> FinishWorkflowResponse:
        LOG.info(f"Finishing workflow {request}")

        if self.fail:
            self.fail = False
            context.abort(StatusCode.INTERNAL, "some_error")

        assert request.workflowName == "some_name"
        assert request.executionId == "exec_id"
        return FinishWorkflowResponse()

    def ReadStdSlots(
        self, request: ReadStdSlotsRequest, context: grpc.ServicerContext
    ) -> Iterator[ReadStdSlotsResponse]:
        LOG.info(f"Registered listener")

        if self.fail:
            self.fail = False
            context.abort(StatusCode.INTERNAL, "some_error")

        yield ReadStdSlotsResponse(
            stdout=ReadStdSlotsResponse.Data(data=("Some stdout",))
        )
        yield ReadStdSlotsResponse(
            stderr=ReadStdSlotsResponse.Data(data=("Some stderr",))
        )


class GrpcRuntimeTests(TestCase):
    def setUp(self) -> None:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.mock = WorkflowServiceMock()
        add_LzyWorkflowServiceServicer_to_server(self.mock, self.server)
        self.server.add_insecure_port("localhost:12345")
        self.server.start()

        key = RSA.generate(2048)
        fd, name = tempfile.mkstemp()
        with open(name, "wb") as f:
            f.write(key.export_key("PEM"))

        self.__key_path = name

    def tearDown(self) -> None:
        self.server.stop(10)
        self.server.wait_for_termination()

    def test_simple(self):
        runtime = GrpcRuntime("ArtoLord", "localhost:12345", self.__key_path)
        lzy = Lzy(runtime=runtime)

        with lzy.workflow("some_name"):
            self.assertIsNotNone(lzy.storage_registry.default_config())

    def test_error(self):
        runtime = GrpcRuntime("ArtoLord", "localhost:12345", self.__key_path)
        lzy = Lzy(runtime=runtime)
        self.mock.fail = True
        with self.assertRaises(expected_exception=Exception):
            with lzy.workflow("some_name"):
                self.assertIsNotNone(lzy.storage_registry.default_config())
        self.assertIsNone(lzy.storage_registry.default_config())

    def test_startup(self):
        def test(a: str, *, b: File) -> str:
            with b.open("r") as f:
                return a + f.readline()

        _, arg_file = tempfile.mkstemp()
        _, kwarg_file = tempfile.mkstemp()
        _, ret_file = tempfile.mkstemp()
        _, data_file = tempfile.mkstemp()

        file = File(data_file)
        with open(data_file, "w") as f:
            f.write("2")
        ser = DefaultSerializerRegistry()

        with open(arg_file, "wb") as arg, open(kwarg_file, "wb") as kwarg:
            ser.find_serializer_by_type(str).serialize("4", arg)
            ser.find_serializer_by_type(File).serialize(file, kwarg)

        startup._lzy_mount = ""

        req = startup.ProcessingRequest(
            serializers=ser,
            op=test,
            args_paths=[(str, arg_file)],
            kwargs_paths={"b": (File, kwarg_file)},
            output_paths=[ret_file],
        )

        startup.main(pickle(req))

        with open(ret_file, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)


@op
def a(b: int) -> int:
    return b + 1


@op
def c(d: int) -> str:
    return str(d)


@dataclasses.dataclass
@whiteboard("wb")
class Wb:
    b: str
    a: int = 1


class SnapshotTests(TestCase):
    def setUp(self) -> None:
        self.service = ThreadedMotoServer(port=12345)
        self.service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(self._create_bucket())

    def tearDown(self) -> None:
        self.service.stop()

    async def _create_bucket(self) -> None:
        async with aioboto3.Session().client(
            "s3",
            aws_access_key_id="aaa",
            aws_secret_access_key="aaa",
            endpoint_url=self.endpoint_url,
        ) as s3:
            await s3.create_bucket(Bucket="bucket")

    def test_simple(self):

        storage_config = storage.StorageConfig(
            bucket="bucket",
            credentials=storage.AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )

        storages = DefaultStorageRegistry()
        storages.register_storage("storage", storage_config, True)

        serializers = DefaultSerializerRegistry()

        snapshot = DefaultSnapshot(storages, serializers)

        entry = snapshot.create_entry(str)

        asyncio.run(snapshot.put_data(entry.id, "some_str"))
        ret = asyncio.run(snapshot.get_data(entry.id))

        self.assertEqual(Just("some_str"), ret)

    def test_local(self):
        storage_config = storage.StorageConfig(
            bucket="bucket",
            credentials=storage.AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )

        storages = DefaultStorageRegistry()
        storages.register_storage("storage", storage_config, True)

        lzy = Lzy(storage_registry=storages, runtime=LocalRuntime())

        with lzy.workflow("") as wf:
            l = a(41)

            l2 = c(l)
            l3 = c(l)

            wf.barrier()

            self.assertEqual(l2, "42")
            self.assertEqual(l3, "42")

    def test_presigned_url(self):
        storage_config = storage.StorageConfig(
            bucket="bucket",
            credentials=storage.AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )

        storages = DefaultStorageRegistry()
        storages.register_storage("storage", storage_config, True)

        client = storages.default_client()
        url = client.generate_uri("bucket", "12345")
        with BytesIO(b"42") as f:
            asyncio.run(client.write(url, f))

        presigned_url = asyncio.run(client.sign_storage_uri(url))

        response = requests.get(presigned_url, stream=True)
        data = next(iter(response.iter_content(16)))
        self.assertEqual(data, b"42")

    def test_whiteboard(self):
        storage_config = storage.StorageConfig(
            bucket="bucket",
            credentials=storage.AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )

        storages = DefaultStorageRegistry()
        storages.register_storage("storage", storage_config, True)

        lzy = Lzy(storage_registry=storages, runtime=LocalRuntime())
        with lzy.workflow("test") as wf:
            wb = wf.create_whiteboard(Wb)
            self.assertEqual(1, wb.a)
            wb.b = "lol"
            self.assertEqual("lol", wb.b)
            wb.a = 2
            self.assertEqual(2, wb.a)

            with self.assertRaises(AttributeError):
                wb.a = 3

            with self.assertRaises(AttributeError):
                wb.b = ""

    def test_read_whiteboard(self):
        storage_config = storage.StorageConfig(
            bucket="bucket",
            credentials=storage.AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )

        storages = DefaultStorageRegistry()
        storages.register_storage("storage", storage_config, True)
        serializer = DefaultSerializerRegistry()

        snapshot = DefaultSnapshot(storages, serializer)

        e1 = snapshot.create_entry(str)
        e2 = snapshot.create_entry(int)

        LzyEventLoop.run_async(snapshot.put_data(e1.id, "42"))
        LzyEventLoop.run_async(snapshot.put_data(e2.id, 42))

        wb_desc = Whiteboard(
            id="wb_id",
            name="wb",
            tags=["1", "2"],
            namespace="namespace",
            status=Whiteboard.Status.FINALIZED,
            storage=Storage(
                name="storage"
            ),
            fields=[
                WhiteboardField(
                    status=WhiteboardField.Status.FINALIZED,
                    info=WhiteboardFieldInfo(
                        name="a",
                        linkedState=WhiteboardFieldInfo.LinkedField(
                            scheme=DataScheme(
                                dataFormat=e1.data_scheme.data_format,
                                schemeFormat=e1.data_scheme.schema_format,
                                schemeContent=e1.data_scheme.schema_content,
                                metadata=e1.data_scheme.meta
                            ),
                            storageUri=e1.storage_url
                        )
                    )
                ),
                WhiteboardField(
                    status=WhiteboardField.Status.FINALIZED,
                    info=WhiteboardFieldInfo(
                        name="b",
                        linkedState=WhiteboardFieldInfo.LinkedField(
                            scheme=DataScheme(
                                dataFormat=e2.data_scheme.data_format,
                                schemeFormat=e2.data_scheme.schema_format,
                                schemeContent=e2.data_scheme.schema_content,
                                metadata=e2.data_scheme.meta
                            ),
                            storageUri=e2.storage_url
                        )
                    )
                )
            ]
        )

        wb = _ReadOnlyWhiteboard(storage=storages, serializers=serializer, wb=wb_desc)

        self.assertEqual("42", wb.a)
        self.assertEqual(42, wb.b)

        with self.assertRaises(AttributeError):
            err = wb.c
