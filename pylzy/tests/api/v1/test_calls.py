from dataclasses import dataclass
from typing import Optional, List, Tuple, Union
from unittest import TestCase

from pure_protobuf.dataclasses_ import message, field
from pure_protobuf.types import int32
from pydantic import ValidationError

from lzy.api.v1 import Lzy, op
from lzy.api.v1.signatures import FuncSignature
from lzy.api.v1.utils.proxy_adapter import materialized
from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock


@op
def returns_int() -> int:
    return 1


# noinspection PyUnusedLocal
@op
def one_arg(arg: int) -> str:
    pass


# noinspection PyUnusedLocal
@op
def varargs(c: float, *args, **kwargs) -> None:
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
        # noinspection PyUnusedLocal
        @op
        def one_default(arg: int, b: str = "default") -> str:
            pass

        with self.lzy.workflow("test") as wf:
            one_default(1)

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
        self.assertEqual(Tuple[int, int, int], func.input_types['arg'])

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

    def test_optional_inference(self):
        # noinspection PyUnusedLocal
        @op
        def optional(arg: Optional[str]) -> Optional[str]:
            pass

        with self.lzy.workflow("test") as wf:
            # noinspection PyTypeChecker
            optional(None)
            optional("str")

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(Optional[str], func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(Optional[str], func.input_types['arg'])

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[1].signature.func
        self.assertEqual(Optional[str], func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(Optional[str], func.input_types['arg'])

    def test_union_inference(self):
        # noinspection PyUnusedLocal
        @op
        def union(arg: Union[int, str]) -> Union[int, str]:
            pass

        with self.lzy.workflow("test") as wf:
            # noinspection PyTypeChecker
            union(1)
            union("str")

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(Union[int, str], func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(Union[int, str], func.input_types['arg'])

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[1].signature.func
        self.assertEqual(Union[int, str], func.output_types[0])
        self.assertEqual(1, len(func.input_types))
        self.assertEqual(Union[int, str], func.input_types['arg'])

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
        with self.lzy.workflow("test") as wf:
            varargs(2.0, *(1, 2, 3), **{'a': "a", 'b': 2})

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(6, len(func.input_types))
        self.assertEqual(str, func.input_types['a'])
        self.assertEqual(int, func.input_types['b'])
        self.assertEqual(float, func.input_types['c'])
        self.assertEqual(('a', 'b'), func.kwarg_names)

    def test_varargs_proxy(self):
        with self.lzy.workflow("test") as wf:
            i = returns_int()
            varargs(2.0, *(i,), **{'a': i})

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[1].signature.func
        self.assertEqual(3, len(func.input_types))
        self.assertEqual(int, func.input_types['a'])
        self.assertEqual(float, func.input_types['c'])
        self.assertEqual(('a',), func.kwarg_names)

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

    def test_pure_proto(self):
        @message
        @dataclass
        class MessageClass:
            string_field: str = field(1, default="")
            list_field: List[int32] = field(2, default_factory=list)

        # noinspection PyUnusedLocal
        @op
        def fun1(a: MessageClass) -> MessageClass:
            pass

        @op
        def fun2() -> MessageClass:
            pass

        with self.lzy.workflow("test") as wf:
            fun1(fun2())

        # noinspection PyUnresolvedReferences
        self.assertEqual(2, len(wf.owner.runtime.calls))

    def test_raises(self):
        @op
        def raises() -> int:
            raise RuntimeError("Bad exception")

        with self.lzy.workflow("test") as wf:
            raises()

        # noinspection PyUnresolvedReferences
        func: FuncSignature = wf.owner.runtime.calls[0].signature.func
        self.assertEqual(int, func.output_types[0])

    def test_no_return_hint(self):
        with self.assertRaisesRegex(TypeError, "return type is not annotated*"):
            @op
            def no_hint():
                pass
