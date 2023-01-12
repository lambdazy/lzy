import logging
import os
import tempfile
from concurrent import futures
from unittest import TestCase

# noinspection PyPackageRequirements
import grpc.aio
# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA

import lzy.api.v1.startup as startup
from ai.lzy.v1.workflow.workflow_service_pb2_grpc import (
    add_LzyWorkflowServiceServicer_to_server,
)
from api.v1.mocks import WorkflowServiceMock, WhiteboardIndexClientMock
from lzy.api.v1 import Lzy
from lzy.api.v1.utils.pickle import pickle
from lzy.logs.config import get_logging_config, get_logger
from lzy.serialization.registry import LzySerializerRegistry
from lzy.types import File

logging.basicConfig(level=logging.DEBUG)

_LOG = get_logger(__name__)


class GrpcRuntimeTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # setup PYTHONPATH for child processes used in LocalRuntime
        pylzy_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
        os.environ["PYTHONPATH"] = pylzy_directory + ":" + pylzy_directory + "/tests"

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
        lzy = Lzy(whiteboard_client=WhiteboardIndexClientMock())
        lzy.auth(user="ArtoLord", key_path=self.__key_path, endpoint="localhost:12345")

        with lzy.workflow("some_name"):
            self.assertIsNotNone(lzy.storage_registry.default_config())

    def test_error(self):
        lzy = Lzy(whiteboard_client=WhiteboardIndexClientMock())
        lzy.auth(user="ArtoLord", key_path=self.__key_path, endpoint="localhost:12345")

        self.mock.fail = True
        with self.assertRaises(expected_exception=Exception):
            with lzy.workflow("some_name"):
                self.assertIsNotNone(lzy.storage_registry.default_config())
        self.assertIsNone(lzy.storage_registry.default_config())

    def test_startup(self):
        # noinspection PyShadowingNames
        def test(a: str, *, b: File) -> str:
            # noinspection PyShadowingNames
            with b.open("r") as f:
                return a + f.readline()

        _, arg_file = tempfile.mkstemp()
        _, kwarg_file = tempfile.mkstemp()
        _, ret_file = tempfile.mkstemp()
        _, data_file = tempfile.mkstemp()

        file = File(data_file)
        with open(data_file, "w") as f:
            f.write("2")
        ser = LzySerializerRegistry()

        with open(arg_file, "wb") as arg, open(kwarg_file, "wb") as kwarg:
            ser.find_serializer_by_type(str).serialize("4", arg)
            ser.find_serializer_by_type(File).serialize(file, kwarg)

        startup._lzy_mount = ""

        req = startup.ProcessingRequest(
            get_logging_config(),
            serializers=ser.imports(),
            op=test,
            args_paths=[(str, arg_file)],
            kwargs_paths={"b": (File, kwarg_file)},
            output_paths=[(str, ret_file)],
        )

        startup.main(pickle(req))

        with open(ret_file, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)
