from typing import List
from unittest import TestCase

from lzy.api import op, LzyEnv


class BaseApiTests(TestCase):
    def test_lazy_and_eager_ops(self):
        # Arrange
        self.sink_done = False

        # @op(input_types=(), output_type=str)
        @op
        def source() -> str:
            return "Are you interested in using a neural network to generate text?"

        # @op(input_types=(str,), output_type=list)
        @op
        def process(p: str) -> List[str]:
            res = []
            for sp in p.split(' '):
                res.append(sp)
            return res

        # noinspection PyUnusedLocal
        # @op(input_types=(list,), output_type=type(None))
        @op
        def sink(p: List[str]) -> None:
            self.sink_done = True

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(local=True) as env:
            s = source()
            r = process(s)
            sink(r)
            done_after_ops = self.sink_done

        # Assert
        self.assertFalse(done_after_ops)
        self.assertTrue(self.sink_done)

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(eager=True, local=True) as env:
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
        # @op(input_types=(), output_type=int)
        @op
        def get_int() -> int:
            return 10

        # @op(input_types=(int,), output_type=float)
        @op
        def add_float(val: int) -> float:
            return val + 0.5

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(local=True) as env:
            n = get_int()
            s = 0.0
            for i in range(n):
                s += add_float(i)

        # Assert
        self.assertEqual(10, n)
        self.assertAlmostEqual(50, s)

    def test_specified_output_types(self):
        # Arrange
        # @op(input_types=(), output_type=int)
        @op(output_type=int)
        def get_int():
            return 10

        # @op(input_types=(int,), output_type=float)
        @op
        def add_float(val: int) -> float:
            return val + 0.5

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(local=True) as env:
            n = get_int()
            s = 0.0
            for i in range(n):
                s += add_float(i)

        # Assert
        self.assertEqual(10, n)
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

        # @op(input_types=(), output_type=A)
        @op
        def a() -> A:
            return A('a')

        # @op(input_types=(), output_type=A)
        @op
        def b(val_a: A) -> B:
            return B(val_a.a(), 'b')

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(local=True) as env:
            a_res = a()
            b_res = b(a_res)

        # Assert
        self.assertEqual('a', a_res.a())
        self.assertEqual('b', b_res.b())

    def test_function_with_none(self):
        n = 0
        @op
        def none_func() -> None:
            nonlocal n
            n = 5
            return None

        @op
        def none_receiver_func(ahah: None) -> int:
            self.assertIsNone(ahah)
            nonlocal n
            n = 42
            return n

        # Act
        # noinspection PyUnusedLocal
        with LzyEnv(local=True) as env:
            a_res = none_func()
            b_res = none_receiver_func(a_res)

        # the result depends on the order of execution here
        self.assertEqual(b_res, 42)
        self.assertEqual(n, 42)

    def test_not_annotated_type_error(self):
        with self.assertRaises(TypeError) as _:
            @op
            def not_annotated():
                pass
