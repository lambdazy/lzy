from typing import List, Optional
from unittest import TestCase
import uuid

from lzy.api.v1 import op, LzyLocalEnv

WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())


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
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            s = source()
            r = process(s)
            sink(r)
            done_after_ops = self.sink_done

        # Assert
        self.assertFalse(done_after_ops)
        self.assertTrue(self.sink_done)

        # Act
        # noinspection PyUnusedLocal
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME, eager=True) as env:
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
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
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
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
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
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            a_res = a()
            b_res = b(a_res)

        # Assert
        self.assertEqual('a', a_res.a())
        self.assertEqual('b', b_res.b())

    def test_function_with_none(self):
        variable: int = 0

        @op
        def none_func() -> None:
            nonlocal variable
            variable = 5
            return None

        @op
        def none_receiver_func(ahah: None) -> int:
            self.assertIsNone(ahah)
            nonlocal variable
            variable = 42
            return variable

        # Act
        # noinspection PyUnusedLocal
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            a_res = none_func()
            b_res = none_receiver_func(a_res)

        # the result depends on the order of execution here
        self.assertEqual(b_res, 42)
        self.assertEqual(variable, 42)

    def test_not_annotated_type_error(self):
        with self.assertRaises(TypeError) as _:
            @op
            def not_annotated():
                pass

    def test_kwargs(self):
        @op
        def summing(a: int, b: int) -> int:
            return a + b

        # Act
        # noinspection PyUnusedLocal
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            s = summing(a=1, b=2)

        # the result depends on the order of execution here
        self.assertEqual(s, 3)

    def test_optional(self):
        @op
        def opt() -> Optional[List[str]]:
            return ["str", "str"]

        # Act
        # noinspection PyUnusedLocal
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            s = opt()

        # the result depends on the order of execution here
        self.assertEqual(s, ["str", "str"])

    def test_args_kwargs(self):
        @op
        def opt(a, b, *args, **kwargs) -> List[str]:
            return [a, b, args[0], kwargs['s']]

        # Act
        # noinspection PyUnusedLocal
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME) as env:
            s = opt('str', 'str', *('str',), **{'s': 'str'})

        # the result depends on the order of execution here
        self.assertEqual(s, ["str", "str", "str", "str"])
