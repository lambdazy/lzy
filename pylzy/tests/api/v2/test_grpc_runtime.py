import logging
import tempfile
from concurrent import futures
from typing import Generator, Iterator
from unittest import TestCase

import grpc.aio
from Crypto.PublicKey import RSA
from grpc import StatusCode

from ai.lzy.v1.workflow.workflow_pb2 import AmazonCredentials, SnapshotStorage
from ai.lzy.v1.workflow.workflow_service_pb2 import (
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    ExecuteGraphRequest,
    ExecuteGraphResponse,
    FinishWorkflowRequest,
    FinishWorkflowResponse,
    GraphStatusRequest,
    GraphStatusResponse,
    ReadStdSlotsRequest,
    ReadStdSlotsResponse,
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    LzyWorkflowServiceServicer,
    add_LzyWorkflowServiceServicer_to_server,
)
from lzy.api.v2 import Lzy, LzyCall
from lzy.api.v2.exceptions import LzyExecutionException
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
from lzy.api.v2.startup import ProcessingRequest, main
from lzy.api.v2.utils._pickle import pickle
from lzy.serialization.api import SerializersRegistry
from lzy.serialization.registry import DefaultSerializersRegistry
from lzy.serialization.types import File

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
            internalSnapshotStorage=SnapshotStorage(
                bucket="",
                amazon=AmazonCredentials(endpoint="", accessToken="", secretToken=""),
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

    def ExecuteGraph(
        self, request: ExecuteGraphRequest, context: grpc.ServicerContext
    ) -> ExecuteGraphResponse:
        if self.fail:
            self.fail = False
            context.abort(StatusCode.INTERNAL, "some_error")
        pass

    def GraphStatus(
        self, request: GraphStatusRequest, context: grpc.ServicerContext
    ) -> GraphStatusResponse:
        if self.fail:
            self.fail = False
            context.abort(StatusCode.INTERNAL, "some_error")
        pass


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
        lzy = Lzy()
        runtime.start(lzy.workflow("some_name"))

        self.assertIsNotNone(lzy.storage_registry.get_default_credentials())

        runtime.destroy()

        self.assertIsNone(lzy.storage_registry.get_default_credentials())

    def test_error(self):
        runtime = GrpcRuntime("ArtoLord", "localhost:12345", self.__key_path)
        lzy = Lzy()
        self.mock.fail = True
        with self.assertRaises(expected_exception=RuntimeError):
            runtime.start(lzy.workflow("some_name"))
        self.mock.fail = False

        self.assertIsNone(lzy.storage_registry.get_default_credentials())

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
        ser = DefaultSerializersRegistry()

        with open(arg_file, "wb") as arg, open(kwarg_file, "wb") as kwarg:
            ser.find_serializer_by_type(str).serialize("4", arg)
            ser.find_serializer_by_type(File).serialize(file, kwarg)

        req = ProcessingRequest(
            serializers=ser,
            op=test,
            args_paths=[(str, arg_file)],
            kwargs_paths={"b": (File, kwarg_file)},
            output_paths=[ret_file]
        )

        main(pickle(req))

        with open(ret_file, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)
