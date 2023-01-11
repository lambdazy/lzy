import asyncio
import uuid
from typing import List
from unittest import TestCase, skip

# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from lzy.api.v1 import Lzy, op, LocalRuntime
from lzy.storage.api import StorageConfig, AmazonCredentials
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
def list2list(a: List[int]) -> List[str]:
    return [str(i) for i in a]


@op
def inc(numb: int) -> int:
    return numb + 1


def entry_id(lazy_proxy):
    return lazy_proxy.lzy_call.entry_id


class LzyWorkflowTests(TestCase):
    def setUp(self):
        self.service = ThreadedMotoServer(port=12345)
        self.service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(self.endpoint_url))

        self.workflow_name = "workflow_" + str(uuid.uuid4())
        self.lzy = Lzy(runtime=LocalRuntime())

        storage_config = StorageConfig(
            bucket="bucket",
            credentials=AmazonCredentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )
        self.lzy.storage_registry.register_storage('default', storage_config, True)

    def tearDown(self) -> None:
        self.service.stop()

    def test_simple_graph(self):
        with self.lzy.workflow(self.workflow_name):
            l = [1, 2, 3]
            result = list2list(l)
            self.assertEqual([str(i) for i in l], result)

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
