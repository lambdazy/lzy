from typing import List
from unittest import TestCase

from lzy.api import op, LzyEnv


class BaseApiTests(TestCase):
    def test_lazy_and_eager_ops(self):
        # Arrange
        self.sink_done = False

        @op
        def source() -> str:
            return "Are you interested in using a neural network to generate text?"

        @op
        def process(p: str) -> List[str]:
            res = []
            for sp in p.split(' '):
                res.append(sp)
            return res

        # noinspection PyUnusedLocal
        @op
        def sink(p: List[str]) -> None:
            self.sink_done = True

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv() as env:
            s = source()
            r = process(s)
            sink(r)
            done_after_ops = self.sink_done

        # Assert
        self.assertFalse(done_after_ops)
        self.assertTrue(self.sink_done)

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(eager=True) as env:
            s = source()
            r = process(s)
            sink(r)
            done_after_ops = self.sink_done

        # Assert
        self.assertTrue(done_after_ops)
        self.assertTrue(self.sink_done)

        del self.sink_done

    def test_primitive_proxy(self):
        # Arrange
        @op
        def get_int() -> int:
            return 10

        @op
        def add_float(val: int) -> float:
            return val + 0.5

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv() as env:
            n = get_int()
            s = 0.0
            for i in range(n):
                s += add_float(i)

        # Assert
        self.assertAlmostEqual(50, s)

    def test_custom_classes(self):
        # Arrange
        class A:
            def __init__(self, val: str):
                self._a = val

            def a(self) -> str:
                return self._a

        class B(A):
            def __init__(self, val_a: str, val_b: str):
                super().__init__(val_a)
                self._b = val_b

            def b(self) -> str:
                return self._b

        @op
        def a() -> A:
            return A('a')

        @op
        def b(val_a: A) -> B:
            return B(val_a.a(), 'b')

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv() as env:
            a_res = a()
            b_res = b(a_res)

        # Assert
        self.assertEquals('a', a_res.a())
        self.assertEquals('b', b_res.b())
