from dataclasses import dataclass
from unittest import TestCase

from lzy.api import run


@dataclass
class A:
    a: str


class SimpleRunTests(TestCase):
    def test_run_primitive_func(self):
        def foo():
            return 1

        result = run(lambda: foo(), local=True)
        self.assertEqual(1, result)

    def test_run_complex_func(self):
        def foo(a):
            return A(a + " updated")

        arg = "str"
        result = run(lambda: foo(arg), local=True)
        self.assertEqual("str updated", result.a)
