from typing import Optional, List, Tuple
from unittest import TestCase, skip

from pydantic import ValidationError

from lzy.api.v1 import Lzy, op
from lzy.api.v1.signatures import FuncSignature
from lzy.api.v1.utils.proxy_adapter import materialized
from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock


@op(description="aaa")
def returns_int() -> int:
    return 1


# noinspection PyUnusedLocal
@op
def one_arg(arg: int) -> str:
    pass


# noinspection PyUnusedLocal
@op
def one_default(arg: int, b: Optional[str] = None) -> str:
    pass


# noinspection PyUnusedLocal
@op
def varargs_only(*args) -> str:
    pass


# noinspection PyUnusedLocal
@op
def varkw_only(**kwargs) -> str:
    pass


# noinspection PyUnusedLocal
@op
def call_no_arg_hint(arg) -> str:
    pass


class A:
    pass


# noinspection PyUnusedLocal
@op
def call_custom_class(arg: A) -> A:
    pass


class LzyCallsTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())

    def test_no_args(self):
        with self.assertRaisesRegex(ValidationError, "field required"):
            with self.lzy.workflow("test"):
                one_arg()

    def test_proxy(self):
        with self.lzy.workflow("test") as wf:
            i = returns_int()
            self.assertFalse(materialized(i))
            one_arg(arg=i)

        # noinspection PyUnresolvedReferences
        func1: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(int, func1.output_types[0])
        self.assertEqual(0, len(func1.input_types))

        # noinspection PyUnresolvedReferences
        func2: FuncSignature = wf.owner.runtime.calls[1].signature.func
        self.assertEqual(str, func2.output_types[0])
        self.assertEqual(1, len(func2.input_types))
        self.assertEqual(int, func2.input_types['arg'])

    def test_default(self):
        with self.lzy.workflow("test") as wf:
            one_arg(1)

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(str, func.output_types[0])
        self.assertEqual(1, len(func.input_types))  # defaults are serialized with a function
        self.assertEqual(int, func.input_types['arg'])

    def test_too_much_args(self):
        with self.assertRaisesRegex(ValidationError, "positional arguments expected but"):
            with self.lzy.workflow("test"):
                one_arg(1, "s", 2)

    def test_only_varargs(self):
        with self.assertRaisesRegex(ValidationError, "unexpected keyword arguments"):
            with self.lzy.workflow("test"):
                varargs_only(k=1, s="s", p=2)

    def test_list_inference(self):
        with self.lzy.workflow("test") as wf:
            call_no_arg_hint([1, 2, 3])

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(str, func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(List[int], func.input_types['arg'])

    def test_tuple_inference(self):
        with self.lzy.workflow("test") as wf:
            call_no_arg_hint((1, 2, 3))

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(str, func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(Tuple[int], func.input_types['arg'])

    def test_arg_hint(self):
        # noinspection PyUnusedLocal
        @op
        def call_arg_hint(arg: List[str]) -> str:
            pass

        with self.lzy.workflow("test") as wf:
            # noinspection PyTypeChecker
            call_arg_hint([1, 2, 3])

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(str, func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(List[str], func.input_types['arg'])

    def test_custom_class(self):
        with self.lzy.workflow("test") as wf:
            # noinspection PyTypeChecker
            call_custom_class(A())

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(A, func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(A, func.input_types['arg'])

    def test_custom_class_invalid(self):
        class B:
            pass

        with self.assertRaisesRegex(ValidationError, "instance of A expected"):
            with self.lzy.workflow("test"):
                # noinspection PyTypeChecker
                call_custom_class(B())

    def test_type_from_arg(self):
        @op
        def returns_list_str() -> List[str]:
            pass

        with self.lzy.workflow("test") as wf:
            value = returns_list_str()
            self.assertFalse(materialized(value))
            call_no_arg_hint(value)

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[1].signature.func
        self.assertEqual(str, func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(List[str], func.input_types['arg'])

    def test_returns_none(self):
        @op
        def returns_none() -> None:
            pass

        with self.lzy.workflow("test"):
            # noinspection PyNoneFunctionAssignment
            result = returns_none()
            self.assertIsNone(result)

    def test_none_arguments(self):
        with self.lzy.workflow("test") as wf:
            call_no_arg_hint(None)

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(type(None), func.input_types['arg'])

    def test_varargs(self):
        # noinspection PyUnusedLocal
        @op
        def varargs(c: float, *args, **kwargs) -> None:
            pass

        with self.lzy.workflow("test") as wf:
            varargs(2.0, *(1, 2, 3), **{'a': "a", 'b': 2})

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(6, len(func.input_types))
        self.assertEqual(str, func.input_types['a'])
        self.assertEqual(int, func.input_types['b'])
        self.assertEqual(float, func.input_types['c'])
        self.assertEqual(('a', 'b'), func.kwarg_names)

    def test_multiple_return_values(self):
        # noinspection PyUnusedLocal
        @op
        def multiple_return() -> (int, str):
            pass

        with self.lzy.workflow("test") as wf:
            multiple_return()

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(int, func.output_types[0])
        self.assertEqual(str, func.output_types[1])

    def test_call_no_workflow(self):
        result = returns_int()
        self.assertEqual(1, result)

    @skip
    def test_description(self):
        description = "my favourite func"

        @op(description=description, ram_size_gb=10, python_version="3.9")
        def func() -> None:
            pass

        with self.lzy.workflow("test") as wf:
            one_arg(1)

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(description, call.description)
