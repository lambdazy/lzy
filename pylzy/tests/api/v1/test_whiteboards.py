import asyncio
import json
import tempfile
import uuid
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast, BinaryIO, Tuple, Optional, Union, List
from unittest import TestCase

# noinspection PyPackageRequirements
import grpc
# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA
# noinspection PyPackageRequirements
from google.protobuf.json_format import MessageToJson, ParseDict
# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

# noinspection PyPackageRequirements
from ai.lzy.v1.whiteboard import whiteboard_pb2
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import add_LzyWhiteboardServiceServicer_to_server
from lzy.api.v1 import Lzy, whiteboard, WhiteboardStatus, MISSING_WHITEBOARD_FIELD, op
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.storage.api import Storage, S3Credentials
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.event_loop import LzyEventLoop
from tests.api.v1.mocks import SerializerRegistryMock, NotStablePrimitiveSerializer, NotAvailablePrimitiveSerializer, \
    WhiteboardIndexServiceMock, EnvProviderMock
from tests.api.v1.utils import create_bucket


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


@op
def func() -> int:
    return 42


@op(cache=True)
def concat(num: int, line: str) -> str:
    return f"{line}: {num}"


class WhiteboardTests(TestCase):
    endpoint_url = None
    wb_service_url = None
    key_path = None
    mock = None
    lzy = None
    grpc_server = None
    storage_uri = None
    s3_service = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.s3_service = ThreadedMotoServer(port=12345)
        cls.s3_service.start()
        cls.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(cls.endpoint_url))

        cls.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        cls.mock = WhiteboardIndexServiceMock()
        add_LzyWhiteboardServiceServicer_to_server(cls.mock, cls.grpc_server)
        cls.wb_service_url = "localhost:12346"
        cls.grpc_server.add_insecure_port(cls.wb_service_url)
        cls.grpc_server.start()

        key = RSA.generate(2048)
        fd, cls.key_path = tempfile.mkstemp()
        with open(cls.key_path, "wb") as f:
            f.write(key.export_key("PEM"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.s3_service.stop()
        cls.grpc_server.stop(10)
        cls.grpc_server.wait_for_termination()

    def setUp(self) -> None:
        self.workflow_name = "workflow_" + str(uuid.uuid4())
        self.storage_uri = "s3://bucket/prefix"
        storage_config = Storage(
            uri=self.storage_uri,
            credentials=S3Credentials(self.endpoint_url, access_key_id="", secret_access_key="")
        )

        self.lzy = Lzy(runtime=LocalRuntime(),
                       py_env_provider=EnvProviderMock(),
                       storage_registry=DefaultStorageRegistry())

        self.lzy.storage_registry.register_storage("default", storage_config, default=True)
        self.lzy.auth(user="test_user", key_path=self.key_path, whiteboards_endpoint=self.wb_service_url,
                      endpoint="endpoint")

    def tearDown(self) -> None:
        self.mock.clear_all()

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

    def test_whiteboard_storage_meta(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard)

        fetched_wb = self.lzy.whiteboard(storage_uri=wb.storage_uri)
        self.assertEqual(fetched_wb.id, wb.id)
        self.assertEqual(WhiteboardStatus.FINALIZED, fetched_wb.status)

    def test_whiteboard_manual_change(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard)

        wb_meta_uri = f"{wb.storage_uri}/.whiteboard"

        with tempfile.TemporaryFile() as f:
            LzyEventLoop.run_async(wf.owner.storage_client.read(wb_meta_uri, cast(BinaryIO, f)))
            f.seek(0)
            storage_wb = ParseDict(json.load(f), whiteboard_pb2.Whiteboard())

        changed_wb = whiteboard_pb2.Whiteboard(id=storage_wb.id, name="changed", tags=storage_wb.tags,
                                               fields=storage_wb.fields, storage=storage_wb.storage,
                                               namespace=storage_wb.namespace, status=storage_wb.status,
                                               createdAt=storage_wb.createdAt)

        with tempfile.NamedTemporaryFile() as f:
            f.write(MessageToJson(changed_wb).encode('UTF-8'))
            f.seek(0)
            LzyEventLoop.run_async(wf.owner.storage_client.write(wb_meta_uri, cast(BinaryIO, f)))

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertNotEqual(fetched_wb.name, wb.name)
        self.assertEqual(fetched_wb.name, "changed")

    def test_whiteboard_manual_corrupt(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard)

        wb_meta_uri = f"{wb.storage_uri}/.whiteboard"

        with tempfile.NamedTemporaryFile() as f:
            f.write("not-whiteboard".encode('UTF-8'))
            f.seek(0)
            LzyEventLoop.run_async(wf.owner.storage_client.write(wb_meta_uri, cast(BinaryIO, f)))

        with self.assertRaisesRegex(RuntimeError, "Whiteboard corrupted"):
            self.lzy.whiteboard(id_=wb.id)

    def test_whiteboard_missing_id(self):
        with self.assertRaisesRegex(ValueError, "Neither id nor uri are set"):
            self.lzy.whiteboard()

    def test_whiteboard_mismatched_id(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb1 = wf.create_whiteboard(Whiteboard)
            wb2 = wf.create_whiteboard(Whiteboard)

        with self.assertRaisesRegex(ValueError, "Id mismatch"):
            self.lzy.whiteboard(id_=wb1.id, storage_uri=wb2.storage_uri)

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
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(WhiteboardWithDefaults)
            wb.desc = "str"
            wb.num = func()

        fetched_wb = self.lzy.whiteboard(id_=wb.id)
        self.assertEqual(42, fetched_wb.num)
        self.assertEqual("str", fetched_wb.desc)

    def test_whiteboard_op_result_after_materialization(self):
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
        with self.assertRaisesRegex(ValueError, "Name attribute must be specified"):
            @whiteboard(name=None)
            @dataclass
            class Wb:
                num: int
                desc: str

        with self.assertRaisesRegex(TypeError, "Name attribute is required to be a string"):
            @whiteboard(name=1)
            @dataclass
            class Wb:
                num: int
                desc: str

        with self.assertRaisesRegex(ValueError, "Invalid workflow name. Name can contain only"):
            @whiteboard(name="test test")
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
        registry = self.lzy._Lzy__serializer_registry
        self.lzy._Lzy__serializer_registry = SerializerRegistryMock()
        try:
            with self.assertRaisesRegex(TypeError, "Cannot find serializer for type"):
                with self.lzy.workflow(self.workflow_name) as wf:
                    wf.create_whiteboard(WhiteboardWithDefaults)
        finally:
            self.lzy._Lzy__serializer_registry = registry

    def test_whiteboard_serializer_unavailable(self):
        serializers = SerializerRegistryMock()
        serializers.register_serializer(NotAvailablePrimitiveSerializer())
        registry = self.lzy._Lzy__serializer_registry
        self.lzy._Lzy__serializer_registry = serializers
        try:
            with self.assertRaisesRegex(TypeError, "is not available, please install"):
                with self.lzy.workflow(self.workflow_name) as wf:
                    wf.create_whiteboard(WhiteboardWithDefaults)
        finally:
            self.lzy._Lzy__serializer_registry = registry

    def test_whiteboard_serializer_unstable(self):
        serializers = SerializerRegistryMock()
        serializers.register_serializer(NotStablePrimitiveSerializer())
        registry = self.lzy._Lzy__serializer_registry
        self.lzy._Lzy__serializer_registry = serializers
        try:
            with self.assertRaisesRegex(TypeError, "we cannot serialize them in a portable format"):
                with self.lzy.workflow(self.workflow_name) as wf:
                    wf.create_whiteboard(WhiteboardWithDefaults)
        finally:
            self.lzy._Lzy__serializer_registry = registry

    def test_whiteboard_list(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb1 = wf.create_whiteboard(Whiteboard, tags=["a"])
            wb2 = wf.create_whiteboard(Whiteboard, tags=["b"])
            wb1.num = 1
            wb2.num = 1
            wb1.desc = "str"
            wb2.desc = "str"

        whiteboards = list(self.lzy.whiteboards(name="whiteboard_name"))
        self.assertEqual(2, len(whiteboards))
        self.assertEqual(wb1.id, whiteboards[0].id)
        self.assertEqual(wb2.id, whiteboards[1].id)

        whiteboards = list(self.lzy.whiteboards(name="whiteboard_name", tags=["a"]))
        self.assertEqual(1, len(whiteboards))
        self.assertEqual(wb1.id, whiteboards[0].id)

        whiteboards = list(self.lzy.whiteboards(name="whiteboard_name", tags=["b"]))
        self.assertEqual(1, len(whiteboards))
        self.assertEqual(wb2.id, whiteboards[0].id)

        whiteboards = list(self.lzy.whiteboards(tags=["a", "b"]))
        self.assertEqual(2, len(whiteboards))
        self.assertEqual(wb1.id, whiteboards[0].id)
        self.assertEqual(wb2.id, whiteboards[1].id)

        whiteboards = list(self.lzy.whiteboards(tags=["c"]))
        self.assertEqual(0, len(whiteboards))

        prev_day_datetime_local = datetime.now() - timedelta(days=1)
        next_day_datetime_local = prev_day_datetime_local + timedelta(days=1)
        whiteboards = list(self.lzy.whiteboards(not_before=prev_day_datetime_local, not_after=next_day_datetime_local))
        self.assertEqual(2, len(whiteboards))
        self.assertEqual(wb1.id, whiteboards[0].id)
        self.assertEqual(wb2.id, whiteboards[1].id)

        whiteboards = list(self.lzy.whiteboards(not_before=next_day_datetime_local, not_after=prev_day_datetime_local))
        self.assertEqual(0, len(whiteboards))

    def test_whiteboard_entries(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(Whiteboard, tags=["a"])
            wb.num = 42
            wb.desc = "str"
            res = concat(wb.num, wb.desc)

            entry_concat_num = wf.call_queue[0].arg_entry_ids[0]
            entry_concat_desc = wf.call_queue[0].arg_entry_ids[1]

        entry_wb_num = wf.entry_index.get_entry_id(wb.num)
        entry_wb_desc = wf.entry_index.get_entry_id(wb.desc)

        self.assertEqual(entry_wb_num, entry_concat_num)
        self.assertEqual(entry_wb_desc, entry_concat_desc)
        self.assertEqual("str: 42", res)

    def test_whiteboard_with_cache_1(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb_1 = wf.create_whiteboard(Whiteboard)
            wb_2 = wf.create_whiteboard(WhiteboardWithDefaults)
            wb_1.num = 1
            wb_1.desc = "str"
            wb_2.num = wb_1.num
            wb_2.desc = wb_1.desc

            concat(wb_2.num, wb_2.desc)
            concat(1, "str")

            reid_1 = wf.call_queue[0].entry_ids[0]
            reid_2 = wf.call_queue[1].entry_ids[0]

        op_1_result_uri = wf.snapshot.get(reid_1).storage_uri
        op_2_result_uri = wf.snapshot.get(reid_2).storage_uri

        self.assertEqual(op_1_result_uri, op_2_result_uri)

    def test_whiteboard_with_cache_2(self):
        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(WhiteboardWithDefaults)
            wb.desc = "str"
            a = func()
            wb.num = a

            concat(wb.num, wb.desc)
            concat(a, "str")

            reid_1 = wf.call_queue[1].entry_ids[0]
            reid_2 = wf.call_queue[2].entry_ids[0]

        op_1_result_uri = wf.snapshot.get(reid_1).storage_uri
        op_2_result_uri = wf.snapshot.get(reid_2).storage_uri

        self.assertEqual(op_1_result_uri, op_2_result_uri)

    def test_invalid_type_assignment(self):
        with self.assertRaisesRegex(TypeError, "Incompatible types"):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(WhiteboardWithDefaults)
                wb.desc = 2

    def test_tuples(self):
        @whiteboard(name="tuple_wb")
        @dataclass
        class TupleWb:
            t: Tuple[str, str]

        @op
        def returns_tuple_1() -> (str, str):
            return "str1", "str2"

        @op
        def returns_tuple_2() -> Tuple[str, str]:
            return "str1", "str2"

        with self.lzy.workflow(self.workflow_name) as wf:
            wb1 = wf.create_whiteboard(TupleWb)
            wb1.t = returns_tuple_1()

            wb2 = wf.create_whiteboard(TupleWb)
            wb2.t = returns_tuple_2()

        self.assertEqual(("str1", "str2"), self.lzy.whiteboard(id_=wb1.id).t)
        self.assertEqual(("str1", "str2"), self.lzy.whiteboard(id_=wb2.id).t)

    def test_optional(self):
        @whiteboard(name="optional_wb")
        @dataclass
        class OptionalWb:
            field: Optional[int]

        @op
        def returns_none() -> None:
            return None

        with self.lzy.workflow(self.workflow_name) as wf:
            wb_not_none = wf.create_whiteboard(OptionalWb)
            wb_not_none.field = func()

            wb_none_local = wf.create_whiteboard(OptionalWb)
            wb_none_local.field = None

            wb_op_none = wf.create_whiteboard(OptionalWb)
            # noinspection PyNoneFunctionAssignment
            wb_op_none.field = returns_none()

        self.assertEqual(42, self.lzy.whiteboard(id_=wb_not_none.id).field)
        self.assertEqual(None, self.lzy.whiteboard(id_=wb_none_local.id).field)
        self.assertEqual(None, self.lzy.whiteboard(id_=wb_op_none.id).field)

    def test_union(self):
        @whiteboard(name="union_wb")
        @dataclass
        class UnionWb:
            field: Union[int, List[int]]

        @op
        def returns_union() -> Union[int, List[int]]:
            return [1, 2, 3]

        with self.lzy.workflow(self.workflow_name) as wf:
            wb_int = wf.create_whiteboard(UnionWb)
            wb_int.field = func()

            wb_list_local = wf.create_whiteboard(UnionWb)
            wb_list_local.field = [1, 2, 3]

            wb_op_list = wf.create_whiteboard(UnionWb)
            # noinspection PyNoneFunctionAssignment
            wb_op_list.field = returns_union()

        self.assertEqual(42, self.lzy.whiteboard(id_=wb_int.id).field)
        self.assertEqual([1, 2, 3], self.lzy.whiteboard(id_=wb_list_local.id).field)
        self.assertEqual([1, 2, 3], self.lzy.whiteboard(id_=wb_op_list.id).field)

    def test_list(self):
        @whiteboard(name="list_wb")
        @dataclass
        class ListWb:
            field: List[int]

        @op
        def returns_list_int() -> List[int]:
            return [1, 2, 3]

        @op
        def returns_list_str() -> List[str]:
            return ["1", "2", "3"]

        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(ListWb)
            wb.field = returns_list_int()
        self.assertEqual([1, 2, 3], self.lzy.whiteboard(id_=wb.id).field)

        with self.lzy.workflow(self.workflow_name) as wf:
            wb = wf.create_whiteboard(ListWb)
            wb.field = [1, 2, 3]
        self.assertEqual([1, 2, 3], self.lzy.whiteboard(id_=wb.id).field)

        with self.assertRaises(TypeError):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(ListWb)
                wb.field = returns_list_str()

        with self.assertRaises(TypeError):
            with self.lzy.workflow(self.workflow_name) as wf:
                wb = wf.create_whiteboard(ListWb)
                wb.field = ["1", "2", "3"]
