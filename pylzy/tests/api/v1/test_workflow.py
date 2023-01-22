import asyncio
import uuid
from typing import List, Optional, Union, Tuple
from unittest import TestCase, skip

# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from tests.api.v1.mocks import EnvProviderMock
from lzy.api.v1 import Lzy, op, LocalRuntime, materialize
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.utils.proxy_adapter import materialized, is_lzy_proxy
from lzy.storage.api import Storage, S3Credentials
from tests.api.v1.utils import create_bucket


@op
def foo() -> str:
    return "Foo:"


@op
def bar(a: str) -> str:
    return f"{a} Bar:"


@op
def baz(a: str, b: int) -> str:
    return f"{a} Baz({b}):"


@op
def boo(a: str, b: str) -> str:
    return f"{a} {b} Boo"


@op
def inc(numb: int) -> int:
    return numb + 1


@op
def return_list() -> List[dict]:
    return [{}, {}, {}]


def entry_id(lazy_proxy):
    return lazy_proxy.lzy_call.entry_id


class LzyWorkflowTests(TestCase):
    endpoint_url = None
    service = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.service = ThreadedMotoServer(port=12345)
        cls.service.start()
        cls.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(cls.endpoint_url))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.service.stop()

    def setUp(self):
        self.workflow_name = "workflow_" + str(uuid.uuid4())
        self.lzy = Lzy(runtime=LocalRuntime(), py_env_provider=EnvProviderMock())

        storage_config = Storage(
            uri="s3://bucket/prefix",
            credentials=S3Credentials(self.endpoint_url, access_key_id="", secret_access_key="")
        )
        self.lzy.storage_registry.register_storage('default', storage_config, True)

    def test_lists(self):
        @op
        def list2list(a: List[int]) -> List[str]:
            return [str(i) for i in a]

        with self.lzy.workflow(self.workflow_name):
            some_list = [1, 2, 3]
            result = list2list(some_list)
            self.assertEqual([str(i) for i in some_list], result)

    def test_tuple_type(self):
        @op
        def returns_tuple() -> Tuple[str, int]:
            return "str", 42

        with self.lzy.workflow(self.workflow_name):
            a, b = returns_tuple()

        self.assertEqual("str", a)
        self.assertEqual(42, b)

    def test_tuple_type_of_lists(self):
        @op
        def returns_tuple() -> Tuple[List, List]:
            return [1], [2]

        with self.lzy.workflow(self.workflow_name):
            a, b = returns_tuple()

        self.assertEqual([1], a)
        self.assertEqual([2], b)

    def test_tuple_type_of_typed_lists(self):
        @op
        def returns_tuple() -> Tuple[List[int], List[int]]:
            return [1], [2]

        with self.lzy.workflow(self.workflow_name):
            a, b = returns_tuple()

        self.assertEqual([1], a)
        self.assertEqual([2], b)

    def test_tuple_with_ellipses(self):
        @op
        def returns_tuple() -> Tuple[List[int], ...]:
            return [1], [2]

        with self.lzy.workflow(self.workflow_name):
            a, b = returns_tuple()

        self.assertEqual([1], a)
        self.assertEqual([2], b)

    def test_tuple_type_short(self):
        @op
        def returns_tuple() -> (str, int):
            return "str", 42

        with self.lzy.workflow(self.workflow_name):
            a, b = returns_tuple()

        self.assertEqual("str", a)
        self.assertEqual(42, b)

    def test_optional_return(self):
        @op
        def optional_not_none() -> Optional[str]:
            return "s"

        @op
        def optional_none() -> Optional[str]:
            return None

        with self.lzy.workflow(self.workflow_name):
            n = optional_none()
            s = optional_not_none()

            self.assertFalse(materialized(s))
            self.assertIsNotNone(s)
            self.assertEqual("s", s)
            self.assertTrue(materialized(s))

            self.assertFalse(materialized(n))
            if n:
                self.fail()
            self.assertTrue(materialized(n))
            self.assertEqual(None, n)

    def test_optional_arg(self):
        @op
        def optional(a: Optional[str]) -> str:
            if a:
                return "not none"
            return "none"

        with self.lzy.workflow(self.workflow_name):
            n = optional(None)
            nn = optional("nn")

            self.assertEqual("not none", nn)
            self.assertEqual("none", n)

    def test_union_return(self):
        @op
        def union(p: bool) -> Union[str, int]:
            if p:
                return "str"
            return 42

        with self.lzy.workflow(self.workflow_name):
            s = union(True)
            lint = union(False)

            res = 0
            for i in range(lint):
                res += 1

            self.assertEqual(42, res)
            self.assertEqual(42, lint)
            self.assertEqual("str", s)

    def test_union_arg(self):
        @op
        def is_str(a: Union[str, int]) -> bool:
            if isinstance(a, str):
                return True
            return False

        with self.lzy.workflow(self.workflow_name):
            t = is_str("str")
            f = is_str(1)

            self.assertEqual(True, t)
            self.assertEqual(False, f)

    def test_globals_not_materialized(self):
        with self.lzy.workflow(self.workflow_name):
            # noinspection PyGlobalUndefined
            global s1, s2
            s1 = foo()
            s2 = foo()
            self.assertFalse(materialized(s1))
            self.assertFalse(materialized(s2))

    def test_kwargs(self):
        with self.lzy.workflow("test"):
            f = foo()
            b = bar(a=f)

        self.assertEqual("Foo: Bar:", b)

    def test_return_accept_list(self):
        @op
        def accept_list(lst: List[dict]) -> int:
            return len(lst)

        with self.lzy.workflow("test"):
            a = return_list()
            i = accept_list(a)

        self.assertFalse(materialized(a))
        self.assertFalse(materialized(i))
        self.assertEqual(3, i)

    def test_return_accept_unspecified_list(self):
        # noinspection PyUnusedLocal
        @op
        def accept_list(lst: List, bst: List) -> int:
            return len(lst)

        with self.assertRaises(LzyExecutionException):
            with self.lzy.workflow("test"):
                a = return_list()
                accept_list(a, [{}])

    def test_lazy(self):
        with self.lzy.workflow("test") as wf:
            f = foo()
            b1 = bar(f)
            i = inc(1)
            b2 = baz(b1, i)
            queue_len = len(wf.call_queue)

        self.assertEqual(4, queue_len)
        self.assertFalse(materialized(f))
        self.assertFalse(materialized(b1))
        self.assertFalse(materialized(i))
        self.assertFalse(materialized(b2))
        self.assertEqual("Foo: Bar: Baz(2):", b2)

    def test_eager(self):
        with self.lzy.workflow("test", eager=True) as wf:
            f = foo()
            b1 = bar(f)
            i = inc(1)
            b2 = baz(b1, i)
            queue_len = len(wf.call_queue)

        self.assertEqual(0, queue_len)
        self.assertFalse(is_lzy_proxy(f))
        self.assertEqual(str, type(f))
        self.assertFalse(is_lzy_proxy(b1))
        self.assertEqual(str, type(b1))
        self.assertFalse(is_lzy_proxy(i))
        self.assertEqual(int, type(i))
        self.assertFalse(is_lzy_proxy(b2))
        self.assertEqual(str, type(b2))
        self.assertEqual("Foo: Bar: Baz(2):", b2)

    def test_materialize(self):
        with self.lzy.workflow("test") as wf:
            f = foo()
            b1 = bar(f)
            i = materialize(inc(1))
            b2 = baz(b1, i)
            queue_len = len(wf.call_queue)

        self.assertEqual(1, queue_len)
        self.assertFalse(materialized(f))
        self.assertFalse(materialized(b1))
        self.assertFalse(materialized(b2))
        self.assertFalse(is_lzy_proxy(i))
        self.assertEqual(int, type(i))
        self.assertEqual("Foo: Bar: Baz(2):", b2)

    @skip("WIP")
    def test_barrier(self):
        with self.lzy.workflow(self.workflow_name, False) as workflow:
            f = foo()
            b = bar(f)
            o = boo(b, baz(f, 3))
            workflow.barrier()
            snapshot = workflow.snapshot()
            self.assertEqual("Foo:", snapshot.get(entry_id(f)))
            self.assertEqual("Foo: Bar:", snapshot.get(entry_id(b)))
            self.assertEqual("Foo: Bar: Foo: Baz(3): Boo", snapshot.get(entry_id(o)))

    @skip("WIP")
    def test_iteration(self):
        with self.lzy.workflow(self.workflow_name, False) as workflow:
            snapshot = workflow.snapshot()
            j = inc(0)
            entries = [entry_id(j)]
            for i in range(5):
                j = inc(j)
                entries.append(entry_id(j))
            for entry in entries:
                self.assertIsNone(snapshot.get(entry))
            workflow.barrier()
            for i in range(6):
                self.assertEqual(i + 1, snapshot.get(entries[i]))

    @skip("WIP")
    def test_already_materialized_calls_when_barrier_called(self):
        with self.lzy.workflow(self.workflow_name, False) as workflow:
            snapshot = workflow.snapshot()
            f = foo()
            b = bar(f)
            print(b)

            o = boo(b, baz(f, 3))

            self.assertEqual("Foo:", snapshot.get(entry_id(f)))
            self.assertEqual("Foo: Bar:", snapshot.get(entry_id(b)))
            self.assertIsNone(snapshot.get(entry_id(o)))

            workflow.barrier()
            self.assertEqual("Foo: Bar: Foo: Baz(3): Boo", snapshot.get(entry_id(o)))

    @skip("WIP")
    def test_simultaneous_workflows_are_not_supported(self):
        with self.assertRaises(RuntimeError) as context:
            with self.lzy.workflow(self.workflow_name, False) as workflow1:
                with self.lzy.workflow(self.workflow_name, False) as workflow2:
                    pass
            self.assertTrue(
                "Simultaneous workflows are not supported" in str(context.exception)
            )
