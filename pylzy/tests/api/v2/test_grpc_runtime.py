import asyncio
import logging
import tempfile
from concurrent import futures
from typing import Iterator
from unittest import TestCase

import aioboto3
import grpc.aio
from Crypto.PublicKey import RSA
from grpc import StatusCode
from moto.server import ThreadedMotoServer

from ai.lzy.v1.common.s3_pb2 import AmazonS3Endpoint, S3Locator
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
from lzy.api.v2 import Lzy, LzyWorkflow, op
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
from lzy.api.v2.snapshot import DefaultSnapshot
from lzy.api.v2.startup import ProcessingRequest, main
from lzy.api.v2.utils._pickle import pickle
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

        self.assertIsNone(lzy.storage_registry.default_config())

    def test_error(self):
        runtime = GrpcRuntime("ArtoLord", "localhost:12345", self.__key_path)
        lzy = Lzy(runtime=runtime)
        self.mock.fail = True
        with self.assertRaises(expected_exception=RuntimeError):
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

        req = ProcessingRequest(
            serializers=ser,
            op=test,
            args_paths=[(str, arg_file)],
            kwargs_paths={"b": (File, kwarg_file)},
            output_paths=[ret_file],
        )

        main(pickle(req))

        with open(ret_file, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)


@op
def a(b: int) -> int:
    return b + 1


@op
def c(d: int) -> str:
    return str(d)


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

        lzy = Lzy(storage_registry=storages)

        with lzy.workflow("") as wf:
            l = a(41)

            l2 = c(l)
            l3 = c(l)

            wf.barrier()

            self.assertEqual(l2, "42")
            self.assertEqual(l3, "42")
