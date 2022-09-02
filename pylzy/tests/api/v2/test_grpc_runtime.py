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
    FinishWorkflowRequest,
    FinishWorkflowResponse, ReadStdSlotsResponse, ReadStdSlotsRequest, ExecuteGraphRequest, ExecuteGraphResponse,
    GraphStatusRequest, GraphStatusResponse,
)
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    LzyWorkflowServiceServicer,
    add_LzyWorkflowServiceServicer_to_server,
)
from lzy.api.v2 import Lzy, LzyCall
from lzy.api.v2.exceptions import LzyExecutionException
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime

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

        yield ReadStdSlotsResponse(stdout=ReadStdSlotsResponse.Data(
            data=("Some stdout",)
        ))
        yield ReadStdSlotsResponse(stderr=ReadStdSlotsResponse.Data(
            data=("Some stderr",)
        ))

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
        with self.assertRaises(expected_exception=LzyExecutionException):
            runtime.start(lzy.workflow("some_name"))
        self.mock.fail = False

        self.assertIsNone(lzy.storage_registry.get_default_credentials())


