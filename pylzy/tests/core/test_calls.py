import logging

from dataclasses import dataclass
from typing import Optional, List, Tuple, Union

import pytest

from pure_protobuf.dataclasses_ import message, field
from pure_protobuf.types import int32

from lzy.api.v1 import op
from lzy.api.v1.signatures import FuncSignature
from lzy.api.v1.utils.proxy_adapter import materialized


@op
def returns_int() -> int:
    return 1


@op
def one_arg(arg: int) -> str:
    pass


@op
def varargs(c: float, *args, **kwargs) -> None:
    pass


@op
def varargs_only(*args) -> str:
    pass


@op
def varkw_only(**kwargs) -> str:
    pass


@op
def call_no_arg_hint(arg) -> str:
    pass


class A:
    pass


@op
def call_custom_class(arg: A) -> A:
    pass


def test_no_args(lzy):
    with pytest.raises(KeyError, match="is required but not provided"):
        with lzy.workflow("test"):
            one_arg()


def test_no_args_default(lzy):
    @op
    def one_arg_default(arg: int = 1) -> str:
        pass

    with lzy.workflow("test") as wf:
        one_arg_default()

    func1: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func1.output_types[0]
    assert 0 == len(func1.input_types)


def test_proxy(lzy):
    with lzy.workflow("test") as wf:
        i = returns_int()
        assert materialized(i) is False
        one_arg(arg=i)

    func1: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert int == func1.output_types[0]
    assert 0 == len(func1.input_types)

    func2: FuncSignature = wf.owner.runtime.calls[1].signature.func
    assert str == func2.output_types[0]
    assert 1 == len(func2.input_types)
    assert int == func2.input_types['arg']


def test_default(lzy):
    @op
    def one_default(arg: int, b: str = "default") -> str:
        pass

    with lzy.workflow("test") as wf:
        one_default(1)

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)  # defaults are serialized with a function
    assert int == func.input_types['arg']


def test_too_much_args(lzy):
    with pytest.raises(KeyError, match="Unexpected argument"):
        with lzy.workflow("test"):
            one_arg(1, "s", 2)


def test_only_varargs(lzy):
    with pytest.raises(KeyError, match="Unexpected key argument"):
        with lzy.workflow("test"):
            varargs_only(k=1, s="s", p=2)


def test_invalid_kwarg(lzy):
    with pytest.raises(KeyError, match="Unexpected key argument"):
        with lzy.workflow("test"):
            one_arg(invalid_arg=1)


def test_list_inference(lzy):
    with lzy.workflow("test") as wf:
        call_no_arg_hint([1, 2, 3])

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)
    assert List[int] == func.input_types['arg']


def test_tuple_inference(lzy):
    with lzy.workflow("test") as wf:
        call_no_arg_hint((1, 2, 3))

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Tuple[int, int, int] == func.input_types['arg']


def test_large_tuple_inference(lzy):
    with lzy.workflow("test") as wf:
        call_no_arg_hint(tuple(i for i in range(1000)))

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Tuple[int, ...] == func.input_types['arg']


def test_arg_hint(lzy):
    @op
    def call_arg_hint(arg: List[str]) -> str:
        pass

    with lzy.workflow("test") as wf:
        call_arg_hint(["1", "2", "3"])

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)
    assert List[str] == func.input_types['arg']


def test_arg_hint_invalid_list_arg(lzy):
    @op
    def call_arg_hint(arg: List[str]) -> str:
        pass

    with pytest.raises(TypeError, match="Invalid types"):
        with lzy.workflow("test"):
            call_arg_hint([1, 2, 3])


def test_optional_inference(lzy):
    @op
    def optional(arg: Optional[str]) -> Optional[str]:
        pass

    with lzy.workflow("test") as wf:
        optional(None)
        optional("str")

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert Optional[str] == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Optional[str] == func.input_types['arg']

    func: FuncSignature = wf.owner.runtime.calls[1].signature.func
    assert Optional[str] == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Optional[str] == func.input_types['arg']


def test_union_inference(lzy):
    @op
    def union(arg: Union[int, str]) -> Union[int, str]:
        pass

    with lzy.workflow("test") as wf:
        union(1)
        union("str")

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert Union[int, str] == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Union[int, str] == func.input_types['arg']

    func: FuncSignature = wf.owner.runtime.calls[1].signature.func
    assert Union[int, str] == func.output_types[0]
    assert 1 == len(func.input_types)
    assert Union[int, str] == func.input_types['arg']


def test_custom_class(lzy):
    with lzy.workflow("test") as wf:
        call_custom_class(A())

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert A == func.output_types[0]
    assert 1 == len(func.input_types)
    assert A == func.input_types['arg']


def test_custom_class_invalid(lzy):
    class B:
        pass

    with pytest.raises(TypeError, match="Invalid types"):
        with lzy.workflow("test"):
            call_custom_class(B())


def test_call_argument_invalid(lzy):
    with pytest.raises(TypeError, match="Invalid types"):
        with lzy.workflow("test"):
            i = returns_int()
            call_custom_class(i)


def test_type_from_arg(lzy):
    @op
    def returns_list_str() -> List[str]:
        pass

    with lzy.workflow("test") as wf:
        value = returns_list_str()
        assert materialized(value) is False
        call_no_arg_hint(value)

    func: FuncSignature = wf.owner.runtime.calls[1].signature.func
    assert str == func.output_types[0]
    assert 1 == len(func.input_types)
    assert List[str] == func.input_types['arg']


def test_returns_none(lzy):
    @op
    def returns_none() -> None:
        pass

    with lzy.workflow("test"):
        result = returns_none()
        assert result is None


def test_none_arguments(lzy):
    with lzy.workflow("test") as wf:
        call_no_arg_hint(None)

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert 1 == len(func.input_types)
    assert func.input_types['arg'] is type(None)


def test_varargs(lzy):
    with lzy.workflow("test") as wf:
        varargs(2.0, *(1, 2, 3), **{'a': "a", 'b': 2})

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert 6 == len(func.input_types)
    assert str == func.input_types['a']
    assert int == func.input_types['b']
    assert float == func.input_types['c']
    assert ('a', 'b') == func.kwarg_names


def test_varargs_proxy(lzy):
    with lzy.workflow("test") as wf:
        i = returns_int()
        varargs(2.0, *(i,), **{'a': i})

    func: FuncSignature = wf.owner.runtime.calls[1].signature.func
    assert 3 == len(func.input_types)
    assert int == func.input_types['a']
    assert float == func.input_types['c']
    assert ('a',) == func.kwarg_names


def test_multiple_return_values(lzy):
    @op
    def multiple_return() -> (int, str):
        pass

    with lzy.workflow("test") as wf:
        multiple_return()

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert int == func.output_types[0]
    assert str == func.output_types[1]


def test_call_no_workflow(lzy):
    result = returns_int()
    assert 1 == result


def test_pure_proto(lzy):
    @message
    @dataclass
    class MessageClass:
        string_field: str = field(1, default="")
        list_field: List[int32] = field(2, default_factory=list)

    @op
    def fun1(a: MessageClass) -> MessageClass:
        pass

    @op
    def fun2() -> MessageClass:
        pass

    with lzy.workflow("test") as wf:
        fun1(fun2())

    assert 2 == len(wf.owner.runtime.calls)


def test_raises(lzy):
    @op
    def raises() -> int:
        raise RuntimeError("Bad exception")

    with lzy.workflow("test") as wf:
        raises()

    func: FuncSignature = wf.owner.runtime.calls[0].signature.func
    assert int == func.output_types[0]


def test_no_return_hint():
    with pytest.raises(TypeError, match=r"Return type is not annotated for function `no_hint` at .*"):
        @op
        def no_hint():
            pass


def test_invalid_workflow_name(lzy):
    with pytest.raises(ValueError, match="Invalid workflow name. Name can contain only"):
        with lzy.workflow("test test"):
            pass


def test_invalid_list_type(lzy):
    @op
    def accept_list(lst: List[int]) -> None:
        pass

    with pytest.raises(TypeError):
        with lzy.workflow("test"):
            accept_list(["a", "b", "c"])


def test_docs():
    @op
    def op_with_doc() -> None:
        """
        :return: None is great
        """

    doc = op_with_doc.__doc__
    assert "None is great" in doc


def test_conda_generate_in_cache(lzy):
    @op
    def foo() -> None:
        pass

    foo = foo.with_manual_python_env(
        python_version="3.7.11",
        local_module_paths=[],
        pypi_packages={"pylzy": "0.0.0"}
    )

    with lzy.workflow("test") as wf:
        foo()

    call = wf.owner.runtime.calls[0]
    yaml = call.get_conda_yaml()

    assert "name: py37" in yaml
    assert "pylzy==0.0.0" in yaml


def test_conda_generate_not_in_cache(caplog, lzy):
    version = "3.7.9999"

    @op
    def foo() -> None:
        pass

    foo = foo.with_manual_python_env(
        python_version=version,
        local_module_paths=[],
        pypi_packages={"pylzy": "0.0.0"}
    )

    with lzy.workflow("test") as wf:
        foo()

    call = wf.owner.runtime.calls[0]

    with caplog.at_level(logging.WARNING, logger='lzy.api.v1.env'):
        yaml = call.get_conda_yaml()

    assert any(
        (
            f"Installed python version ({version})" in record.message and
            record.levelno == logging.WARNING
        )
        for record in caplog.records
    )

    assert "name: default" in yaml
    assert "pylzy==0.0.0" in yaml


def test_generate_pypi_index_config(lzy, pypi_index_url, pypi_index_url_testing):
    def get_pip_deps(config):
        dependencies = config.get('dependencies', [])
        pip_deps = [item for item in dependencies if isinstance(item, dict) and 'pip' in item]

        assert len(pip_deps) <= 1

        return pip_deps

    def get_config(func):
        with lzy.workflow("test") as wf:
            func()
        call = wf.owner.runtime.calls[0]
        config = call.generate_conda_config()
        return config

    @op
    def foo() -> None:
        pass

    foo = foo.with_manual_python_env(
        python_version="3.7.11",
        local_module_paths=[],
        pypi_packages={}
    )
    config = get_config(foo)
    pip_deps = get_pip_deps(config)
    assert len(pip_deps) == 1
    assert pip_deps[0]['pip'] == [f'--index-url {pypi_index_url}']

    foo = foo.with_manual_python_env(
        python_version="3.7.11",
        local_module_paths=[],
        pypi_packages={},
        pypi_index_url=pypi_index_url_testing
    )
    config = get_config(foo)
    pip_deps = get_pip_deps(config)
    assert len(pip_deps) == 1
    assert pip_deps[0]['pip'] == [f'--index-url {pypi_index_url_testing}']
