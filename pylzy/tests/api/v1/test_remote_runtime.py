import logging
import tempfile
from concurrent import futures
from typing import Optional
from unittest import TestCase

# noinspection PyPackageRequirements
import grpc.aio
# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA
# noinspection PyPackageRequirements
from grpc.aio import AioRpcError
from lzy.api.v1.workflow import LzyWorkflow

from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    add_LzyWorkflowServiceServicer_to_server,
)
from ai.lzy.v1.long_running.operation_pb2_grpc import (
    add_LongRunningServiceServicer_to_server,
)
from tests.api.v1.mocks import WorkflowServiceMock, OperationsServiceMock, EnvProviderMock
from lzy.api.v1 import Lzy, op
from lzy.whiteboards.index import DummyWhiteboardIndexClient
from lzy.logs.config import get_logger
from lzy.storage.api import Storage

logging.basicConfig(level=logging.DEBUG)

_LOG = get_logger(__name__)


@op
def opa() -> None:
    return


class RemoteRuntimeTests(TestCase):
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

        self.lzy = Lzy(whiteboard_client=DummyWhiteboardIndexClient(), py_env_provider=EnvProviderMock())
        self.lzy.auth(user="ArtoLord", key_path=name, endpoint="localhost:12345")

    def tearDown(self) -> None:
        self.server.stop(10)
        self.server.wait_for_termination()

    def test_start(self):
        with self.lzy.workflow("some_name"):
            self.assertIsNotNone(self.lzy.storage_registry.default_config())
            self.assertTrue(self.mock.started)

    def test_error_on_start(self):
        self.mock.fail_on_start = True
        # noinspection PyUnusedLocal
        flag: bool = False
        # noinspection PyUnusedLocal
        wf: Optional[LzyWorkflow] = None
        with self.assertRaises(AioRpcError):
            with self.lzy.workflow("some_name") as wf:
                flag = True
        self.assertFalse(flag)
        self.assertIsNone(wf)
        self.assertFalse(self.mock.aborted)  # should NOT be aborted if not started

    def test_error_on_execute_graph(self):
        self.mock.fail_on_execute_graph = True
        with self.assertRaises(AioRpcError):
            with self.lzy.workflow("some_name", interactive=False):
                opa()

        self.assertTrue(self.mock.aborted)

    def test_error_on_execute_graph_barrier(self):
        self.mock.fail_on_execute_graph = True
        with self.assertRaises(AioRpcError):
            with self.lzy.workflow("some_name", interactive=False) as wf:
                opa()
                wf.barrier()

        self.assertTrue(self.mock.aborted)

    def test_error_on_get_pools(self):
        self.mock.fail_on_get_pools = True
        with self.assertRaises(AioRpcError):
            with self.lzy.workflow("some_name", interactive=False):
                opa()

        self.assertTrue(self.mock.aborted)

    def test_empty_pools(self):
        self.mock.return_empty_pools = True
        with self.assertRaises(ValueError):
            with self.lzy.workflow("some_name", interactive=False):
                opa()

        self.assertTrue(self.mock.aborted)

    def test_on_get_storage(self):
        self.mock.fail_on_get_storage = True
        with self.assertRaises(AioRpcError):
            with self.lzy.workflow("some_name", interactive=False):
                pass
        self.assertFalse(self.mock.started)

    def test_local_fs_storage_default(self):
        self.lzy.storage_registry.register_storage("local", Storage.fs_storage("/tmp/default_lzy_storage"),
                                                   default=True)
        with self.assertRaises(ValueError):
            with self.lzy.workflow("some_name", interactive=False):
                pass
