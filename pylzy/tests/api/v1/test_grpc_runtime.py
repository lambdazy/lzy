import logging
import os
import tempfile
from concurrent import futures
from unittest import TestCase

# noinspection PyPackageRequirements
import grpc.aio
# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA

from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    add_LzyWorkflowServiceServicer_to_server,
)
from ai.lzy.v1.long_running.operation_pb2_grpc import (
    add_LongRunningServiceServicer_to_server,
)
from api.v1.mocks import WorkflowServiceMock, WhiteboardIndexClientMock, OperationsServiceMock, EnvProviderMock
from lzy.api.v1 import Lzy
from lzy.logs.config import get_logger

logging.basicConfig(level=logging.DEBUG)

_LOG = get_logger(__name__)


class GrpcRuntimeTests(TestCase):
    def setUp(self) -> None:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.ops_mock = OperationsServiceMock()
        add_LongRunningServiceServicer_to_server(self.ops_mock, self.server)
        self.mock = WorkflowServiceMock(self.ops_mock)
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

    def test_start(self):
        lzy = Lzy(whiteboard_client=WhiteboardIndexClientMock(), py_env_provider=EnvProviderMock())
        lzy.auth(user="ArtoLord", key_path=self.__key_path, endpoint="localhost:12345")

        with lzy.workflow("some_name"):
            self.assertIsNotNone(lzy.storage_registry.default_config())
            self.assertTrue(self.mock.created)

    def test_error(self):
        lzy = Lzy(whiteboard_client=WhiteboardIndexClientMock(), py_env_provider=EnvProviderMock())
        lzy.auth(user="ArtoLord", key_path=self.__key_path, endpoint="localhost:12345")

        self.mock.fail = True
        with self.assertRaises(expected_exception=Exception):
            with lzy.workflow("some_name"):
                self.assertIsNotNone(lzy.storage_registry.default_config())
        self.assertIsNone(lzy.storage_registry.default_config())
