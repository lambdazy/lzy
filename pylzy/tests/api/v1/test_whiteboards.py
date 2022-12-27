import asyncio
import os
import tempfile
import uuid
from concurrent import futures
from dataclasses import dataclass
from unittest import TestCase

# noinspection PyPackageRequirements
import grpc
# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA
# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import add_LzyWhiteboardServiceServicer_to_server
from api.v1.mocks import WhiteboardIndexServiceMock, SerializerRegistryMock, NotAvailablePrimitiveSerializer, \
    NotStablePrimitiveSerializer
from api.v1.utils import create_bucket
from lzy.api.v1 import Lzy, whiteboard, WhiteboardStatus, MISSING_WHITEBOARD_FIELD, op
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.storage.api import Storage, S3Credentials


@whiteboard(name="whiteboard_name")
@dataclass
class Whiteboard:
    num: int
    desc: str


@whiteboard(name="whiteboard_name_with_defaults")
@dataclass
class WhiteboardWithDefaults:
    desc: str
    num: int = 10


class WhiteboardTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # setup PYTHONPATH for child processes used in LocalRuntime
        pylzy_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
        os.environ["PYTHONPATH"] = pylzy_directory + ":" + pylzy_directory + "/tests"

    def setUp(self) -> None:
        self.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.mock = WhiteboardIndexServiceMock()
        add_LzyWhiteboardServiceServicer_to_server(self.mock, self.grpc_server)
        wb_service_url = "localhost:12346"
        self.grpc_server.add_insecure_port(wb_service_url)
        self.grpc_server.start()

        self.s3_service = ThreadedMotoServer(port=12345)
        self.s3_service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(self.endpoint_url))

        key = RSA.generate(2048)
        fd, name = tempfile.mkstemp()
        with open(name, "wb") as f:
            f.write(key.export_key("PEM"))

        self.workflow_name = "workflow_" + str(uuid.uuid4())
        self.lzy = Lzy(runtime=LocalRuntime())
        self.lzy.auth(user="test_user", key_path=name, whiteboards_endpoint=wb_service_url, endpoint="endpoint")

        self.storage_uri = "s3://bucket/prefix"
        storage_config = Storage(
            uri=self.storage_uri,
            credentials=S3Credentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )
        self.lzy.storage_registry.register_storage('default', storage_config, True)

    def tearDown(self) -> None:
        self.grpc_server.stop(10)
        self.grpc_server.wait_for_termination()
        self.s3_service.stop()

    def test_whiteboard_attributes(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard, tags=["a", "b"])

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(fetched_wb.id, wb.id)
        self.assertEqual(WhiteboardStatus.FINALIZED, fetched_wb.status)
        self.assertEqual(wb.name, fetched_wb.name)
        self.assertEqual(wb.tags, fetched_wb.tags)
        self.assertTrue(fetched_wb.storage_uri.startswith(f"{self.storage_uri}/whiteboards/whiteboard_name/"))

    def test_whiteboard_local_data(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard)
            wb.num = 1
            wb.desc = "str"

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(1, fetched_wb.num)
        self.assertEqual("str", fetched_wb.desc)

    def test_whiteboard_missing_field(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard)
            wb.num = 1

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(1, fetched_wb.num)
        self.assertEqual(MISSING_WHITEBOARD_FIELD, fetched_wb.desc)

    def test_whiteboard_with_defaults(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(WhiteboardWithDefaults)
            wb.desc = "str"

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(10, fetched_wb.num)
        self.assertEqual("str", fetched_wb.desc)

    def test_whiteboard_op_result(self):
        @op
        def func() -> int:
            return 42

        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(WhiteboardWithDefaults)
            wb.desc = "str"
            wb.num = func()

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(42, fetched_wb.num)
        self.assertEqual("str", fetched_wb.desc)

    def test_whiteboard_op_result_after_materialization(self):
        @op
        def func() -> int:
            return 42

        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(WhiteboardWithDefaults)
            num = func()
            print(num)
            wb.desc = "str"
            wb.num = num

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(42, fetched_wb.num)
        self.assertEqual("str", fetched_wb.desc)

    # noinspection PyTypeChecker,PyUnusedLocal
    def test_invalid_name(self):
        with self.assertRaisesRegex(ValueError, "name attribute must be specified"):
            @whiteboard(name=None)
            @dataclass
            class Wb:
                num: int
                desc: str

        with self.assertRaisesRegex(TypeError, "name attribute is required to be a string"):
            @whiteboard(name=1)
            @dataclass
            class Wb:
                num: int
                desc: str

    def test_create_whiteboard_invalid_type(self):
        @dataclass
        class Wb:
            num: int
            desc: str

        with self.assertRaisesRegex(TypeError,
                                    "Whiteboard class should be annotated with both @whiteboard and @dataclass"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wf.create_whiteboard(Wb)

    def test_whiteboard_set_invalid_attr(self):
        with self.assertRaisesRegex(AttributeError, "No such attribute: nonexistent_field"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(WhiteboardWithDefaults)
                wb.nonexistent_field = "str"

    def test_whiteboard_set_attr_repeatedly(self):
        with self.assertRaisesRegex(AttributeError, "Whiteboard field can be assigned only once"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(WhiteboardWithDefaults)
                wb.desc = "str"
                wb.desc = "str"

    def test_whiteboard_get_nonexistent_field(self):
        with self.assertRaisesRegex(AttributeError, "No such attribute: nonexistent_field"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(WhiteboardWithDefaults)
                print(wb.nonexistent_field)

    def test_whiteboard_get_non_assigned_field(self):
        with self.assertRaisesRegex(AttributeError, "Whiteboard field desc is not assigned"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(WhiteboardWithDefaults)
                print(wb.num)
                print(wb.desc)

    def test_whiteboard_serializer_not_found(self):
        self.lzy._Lzy__serializer_registry = SerializerRegistryMock()
        with self.assertRaisesRegex(TypeError, "Cannot find serializer for type"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wf.create_whiteboard(WhiteboardWithDefaults)

    def test_whiteboard_serializer_unavailable(self):
        serializers = SerializerRegistryMock()
        serializers.register_serializer(NotAvailablePrimitiveSerializer())
        self.lzy._Lzy__serializer_registry = serializers
        with self.assertRaisesRegex(TypeError, "is not available, please install"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wf.create_whiteboard(WhiteboardWithDefaults)

    def test_whiteboard_serializer_unstable(self):
        serializers = SerializerRegistryMock()
        serializers.register_serializer(NotStablePrimitiveSerializer())
        self.lzy._Lzy__serializer_registry = serializers
        with self.assertRaisesRegex(TypeError, "we cannot serialize them in a portable format"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wf.create_whiteboard(WhiteboardWithDefaults)