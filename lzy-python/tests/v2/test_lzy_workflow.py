import uuid
from unittest import TestCase

from lzy.v2.api import op
from lzy.v2.api.lzy import Lzy


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


class LzyWorkflowTests(TestCase):
    def setUp(self):
        self._WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())
        self._lzy = Lzy()

    def test_py_env_modules_selected(self):
        with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow:
            f = foo()
            b = bar(f)
            self.assertEquals("Foo: Bar: Foo: Baz(3): Boo", boo(b, baz(f, 3)))
            workflow.barrier()
