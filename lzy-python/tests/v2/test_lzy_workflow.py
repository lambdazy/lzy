import uuid
from unittest import TestCase

from lzy.api.v2.api import op
from lzy.api.v2.api.lzy import Lzy


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


def entry_id(lazy_proxy):
    return lazy_proxy.lzy_call.entry_id


class LzyWorkflowTests(TestCase):
    def setUp(self):
        self._WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())
        self._lzy = Lzy()

    def test_barrier(self):
        with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow:
            f = foo()
            b = bar(f)
            o = boo(b, baz(f, 3))
            workflow.barrier()
            snapshot = workflow.snapshot()
            self.assertEqual("Foo:", snapshot.get(entry_id(f)))
            self.assertEqual("Foo: Bar:", snapshot.get(entry_id(b)))
            self.assertEqual("Foo: Bar: Foo: Baz(3): Boo", snapshot.get(entry_id(o)))

    def test_iteration(self):
        with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow:
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

    def test_already_materialized_calls_when_barrier_called(self):
        with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow:
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

    def test_simultaneous_workflows_are_not_supported(self):
        with self.assertRaises(RuntimeError) as context:
            with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow1:
                with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow2:
                    pass
            self.assertTrue('Simultaneous workflows are not supported' in str(context.exception))
