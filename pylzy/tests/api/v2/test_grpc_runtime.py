import logging
import tempfile
from concurrent import futures
from unittest import TestCase

import grpc.aio

from ai.lzy.v1.workflow.workflow_pb2 import SnapshotStorage, AmazonCredentials
from ai.lzy.v1.workflow.workflow_service_pb2 import CreateWorkflowRequest, CreateWorkflowResponse, \
    FinishWorkflowResponse, FinishWorkflowRequest
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import LzyWorkflowServiceServicer,\
    add_LzyWorkflowServiceServicer_to_server
from lzy.api.v2 import Lzy
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
from Crypto.PublicKey import RSA


LOG = logging.getLogger(__name__)


class WorkflowServiceMock(LzyWorkflowServiceServicer):

    def CreateWorkflow(self, request: CreateWorkflowRequest, context: grpc.RpcContext) -> CreateWorkflowResponse:
        LOG.info(f"Creating wf {request}")
        return CreateWorkflowResponse(executionId="exec_id", internalSnapshotStorage=SnapshotStorage(
            bucket="",
            amazon=AmazonCredentials(
                endpoint="",
                accessToken="",
                secretToken=""
            )
        ))

    def FinishWorkflow(self, request: FinishWorkflowRequest, context: grpc.RpcContext) -> FinishWorkflowResponse:
        LOG.info(f"Finishing workflow {request}")
        assert request.workflowName == "some_name"
        assert request.executionId == "exec_id"
        return FinishWorkflowResponse()


class GrpcRuntimeTests(TestCase):
    def setUp(self) -> None:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_LzyWorkflowServiceServicer_to_server(WorkflowServiceMock(), self.server)
        self.server.add_insecure_port("localhost:12345")
        self.server.start()

    def tearDown(self) -> None:
        self.server.stop(10)
        self.server.wait_for_termination()

    def test_simple(self):
        key = RSA.generate(2048)
        fd, name = tempfile.mkstemp()
        with open(name, "wb") as f:
            f.write(key.export_key('PEM'))
        runtime = GrpcRuntime("ArtoLord", "localhost:12345", name)
        lzy = Lzy()
        runtime.start(lzy.workflow("some_name"))

        self.assertIsNotNone(lzy.storage_registry.get_default_credentials())

        runtime.destroy()

        self.assertIsNone(lzy.storage_registry.get_default_credentials())