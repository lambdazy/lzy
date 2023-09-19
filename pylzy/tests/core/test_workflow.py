import importlib
import uuid
from typing import List, Optional, Union, Tuple, Dict

import pytest

from lzy.api.v1 import Lzy, op, LocalRuntime
from lzy.api.v1.utils.proxy_adapter import materialized, is_lzy_proxy, materialize


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


@op
def accept_unspecified_list(lst: List) -> int:
    return len(lst)


@op
def accept_int_list(lst: List[int]) -> int:
    return len(lst)


@op
def accept_custom_class_list(lst: List[Lzy]) -> int:
    return len(lst)


@op
def return_list() -> List[int]:
    return [1, 2, 3]


@op
def accept_returns_dict(d: Dict) -> Dict:
    return d


def entry_id(lazy_proxy) -> None:
    return lazy_proxy.lzy_call.entry_id


@pytest.fixture
def lzy():
    # NB: without storage_registry mock and local runtime instead of mock
    return Lzy(runtime=LocalRuntime())


@pytest.fixture
def workflow_name() -> None:
    return f"test_workflow_{uuid.uuid4()}"


def test_lists(lzy, workflow_name) -> None:
    @op
    def list2list(a: List[int]) -> List[str]:
        return [str(i) for i in a]

    with lzy.workflow(workflow_name):
        some_list = [1, 2, 3]
        result = list2list(some_list)
        etalon = [str(i) for i in some_list]
        assert etalon == result


def test_invalid_list_type_returns(lzy, workflow_name) -> None:
    @op
    def invalid_list_type_returns() -> List[str]:
        # noinspection PyTypeChecker
        return [1, 2, 3]

    with pytest.raises(TypeError):
        with lzy.workflow(workflow_name):
            invalid_list_type_returns()


def test_tuple_type(lzy, workflow_name) -> None:
    @op
    def returns_tuple() -> Tuple[str, int]:
        return "str", 42

    with lzy.workflow(workflow_name):
        a, b = returns_tuple()

    assert "str" == a
    assert 42 == b


def test_tuple_type_of_lists(lzy, workflow_name) -> None:
    @op
    def returns_tuple() -> Tuple[List, List]:
        return [1], [2]

    with lzy.workflow(workflow_name):
        a, b = returns_tuple()

    assert [1] == a
    assert [2] == b


def test_tuple_type_of_typed_lists(lzy, workflow_name) -> None:
    @op
    def returns_tuple() -> Tuple[List[int], List[int]]:
        return [1], [2]

    with lzy.workflow(workflow_name):
        a, b = returns_tuple()

    assert [1] == a
    assert [2] == b


def test_tuple_with_ellipses(lzy, workflow_name) -> None:
    @op
    def returns_tuple() -> Tuple[List[int], ...]:
        return [1], [2]

    with lzy.workflow(workflow_name):
        a, b = returns_tuple()

    assert [1] == a
    assert [2] == b


def test_tuple_type_short(lzy, workflow_name) -> None:
    @op
    def returns_tuple() -> (str, int):
        return "str", 42

    with lzy.workflow(workflow_name):
        a, b = returns_tuple()

    assert "str" == a
    assert 42 == b


def test_optional_return(lzy, workflow_name) -> None:
    @op
    def optional_not_none() -> Optional[str]:
        return "s"

    @op
    def optional_none() -> Optional[str]:
        return None

    with lzy.workflow(workflow_name):
        n = optional_none()
        s = optional_not_none()

        assert materialized(s) is False
        assert s is not None
        assert "s" == s
        assert materialized(s)

        assert materialized(n) is False
        if n:
            pytest.fail()
        assert materialized(n) is True
        assert None == n  # noqa: E711


def test_optional_arg(lzy, workflow_name) -> None:
    @op
    def optional(a: Optional[str]) -> str:
        if a:
            return "not none"
        return "none"

    with lzy.workflow(workflow_name):
        n = optional(None)
        nn = optional("nn")

        assert "not none" == nn
        assert "none" == n


def test_union_return(lzy, workflow_name) -> None:
    @op
    def union(p: bool) -> Union[str, int]:
        if p:
            return "str"
        return 42

    with lzy.workflow(workflow_name):
        s = union(True)
        lint = union(False)

        res = 0
        for i in range(lint):
            res += 1

        assert 42 == res
        assert 42 == lint
        assert "str" == s


def test_union_arg(lzy, workflow_name) -> None:
    @op
    def is_str(a: Union[str, int]) -> bool:
        if isinstance(a, str):
            return True
        return False

    with lzy.workflow(workflow_name):
        t = is_str("str")
        f = is_str(1)

        assert True == t  # noqa: E711
        assert False == f  # noqa: E711


def test_globals_not_materialized(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        # noinspection PyGlobalUndefined
        global s1, s2
        s1 = foo()
        s2 = foo()
        assert materialized(s1) is False
        assert materialized(s2) is False


def test_kwargs(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        f = foo()
        b = bar(a=f)

    assert "Foo: Bar:" == b


def test_return_accept_list(lzy, workflow_name) -> None:
    @op
    def accept_list(lst: List[int]) -> int:
        return len(lst)

    with lzy.workflow(workflow_name):
        a = return_list()
        i = accept_list(a)

    assert materialized(a) is False
    assert materialized(i) is False
    assert 3 == i


def test_return_accept_unspecified_list(lzy, workflow_name) -> None:
    # noinspection PyUnusedLocal
    @op
    def accept_list(lst: List, bst: List) -> int:
        return len(lst)

    with lzy.workflow(workflow_name):
        a = return_list()
        lst_len = accept_list(a, [{}])
    assert 3 == lst_len


def test_empty_list(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        a = accept_int_list([])
        b = accept_unspecified_list([])
        c = accept_custom_class_list([])

    assert 0 == a
    assert 0 == b
    assert 0 == c


def test_primitive_to_unspecified_list(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        a = accept_unspecified_list([1, 2, 3])

    assert 3 == a


def test_unspecified_dict(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        res = accept_returns_dict({1: "2"})
    assert {1: "2"} == res


def test_empty_dict(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name):
        res = accept_returns_dict({})
    assert {} == res


def test_empty_specified_dict(lzy, workflow_name) -> None:
    @op
    def accept_returns_specified_dict(d: Dict[int, str]) -> Dict[int, str]:
        return d

    with lzy.workflow(workflow_name):
        res = accept_returns_specified_dict({})
    assert {} == res


def test_specified_dict(lzy, workflow_name) -> None:
    @op
    def accept_returns_specified_dict(d: Dict[int, str]) -> Dict[int, str]:
        return d

    with lzy.workflow(workflow_name):
        res = accept_returns_specified_dict(accept_returns_specified_dict({1: "2"}))
    assert {1: "2"} == res

    with pytest.raises(TypeError):
        with lzy.workflow(workflow_name):
            # noinspection PyTypeChecker
            accept_returns_specified_dict({1: 1})


def test_lazy(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name) as wf:
        f = foo()
        b1 = bar(f)
        i = inc(1)
        b2 = baz(b1, i)
        queue_len = len(wf.call_queue)

    assert 4 == queue_len
    assert materialized(f) is False
    assert materialized(b1) is False
    assert materialized(i) is False
    assert materialized(b2) is False
    assert "Foo: Bar: Baz(2):" == b2


def test_eager(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name, eager=True) as wf:
        f = foo()
        b1 = bar(f)
        i = inc(1)
        b2 = baz(b1, i)
        queue_len = len(wf.call_queue)

    assert 0 == queue_len
    assert is_lzy_proxy(f) is False
    assert str == type(f)
    assert is_lzy_proxy(b1) is False
    assert str == type(b1)
    assert is_lzy_proxy(i) is False
    assert int == type(i)
    assert is_lzy_proxy(b2) is False
    assert str == type(b2)
    assert "Foo: Bar: Baz(2):" == b2


def test_materialize(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name) as wf:
        f = foo()
        b1 = bar(f)
        i = materialize(inc(1))
        b2 = baz(b1, i)
        queue_len = len(wf.call_queue)

    assert 1 == queue_len
    assert materialized(f) is False
    assert materialized(b1) is False
    assert materialized(b2) is False
    assert is_lzy_proxy(i) is False
    assert int == type(i)
    assert "Foo: Bar: Baz(2):" == b2


def test_lazy_args_loading(lzy, workflow_name) -> None:
    @op(lazy_arguments=True)
    def is_arg_type_str(a: str) -> bool:
        return type(a) is str

    with lzy.workflow(workflow_name):
        res = is_arg_type_str("str")
    assert res == False  # noqa: E711


def test_eager_args_loading(lzy, workflow_name) -> None:
    @op(lazy_arguments=False)
    def is_arg_type_str(a: str) -> bool:
        return type(a) is str

    with lzy.workflow(workflow_name):
        res = is_arg_type_str("str")
    assert res == True  # noqa: E711


def test_return_argument(lzy, workflow_name) -> None:
    @op
    def return_argument(arg: str) -> str:
        return arg

    with lzy.workflow(workflow_name):
        res = return_argument("str")
    assert "str" == res


def test_local_startup_import(lzy, workflow_name) -> None:
    @op
    def op_with_import() -> bool:
        # noinspection PyBroadException
        try:
            importlib.import_module("remote")
            importlib.import_module("local")
            importlib.import_module("utils")
            return True
        except Exception:
            return False

    with lzy.workflow(workflow_name):
        res = op_with_import()

    assert False == res  # noqa: E711


@pytest.mark.skip("currently we do not support lazy collections")
def test_lazy_list(lzy, workflow_name) -> None:
    @op
    def return_list_len(strings: List[str], strings2: List[str]) -> (int, int):
        return len(strings), len(strings2)

    with lzy.workflow(workflow_name):
        a = foo()
        b = foo()
        c = foo()
        len1, len2 = return_list_len([a, b, c], strings2=[a, b, c])

    assert 3 == len1
    assert 3 == len2


@pytest.mark.skip("WIP")
def test_barrier(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name, False) as workflow:
        f = foo()
        b = bar(f)
        o = boo(b, baz(f, 3))
        workflow.barrier()
        snapshot = workflow.snapshot()
        assert "Foo:" == snapshot.get(entry_id(f))
        assert "Foo: Bar:" == snapshot.get(entry_id(b))
        assert "Foo: Bar: Foo: Baz(3): Boo" == snapshot.get(entry_id(o))


@pytest.mark.skip("WIP")
def test_iteration(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name, False) as workflow:
        snapshot = workflow.snapshot()
        j = inc(0)
        entries = [entry_id(j)]
        for i in range(5):
            j = inc(j)
            entries.append(entry_id(j))
        for entry in entries:
            assert snapshot.get(entry) is None
        workflow.barrier()
        for i in range(6):
            assert i + 1 == snapshot.get(entries[i])


@pytest.mark.skip("WIP")
def test_already_materialized_calls_when_barrier_called(lzy, workflow_name) -> None:
    with lzy.workflow(workflow_name, False) as workflow:
        snapshot = workflow.snapshot()
        f = foo()
        b = bar(f)

        o = boo(b, baz(f, 3))

        assert "Foo:" == snapshot.get(entry_id(f))
        assert "Foo: Bar:" == snapshot.get(entry_id(b))
        assert snapshot.get(entry_id(o)) is None

        workflow.barrier()
        assert "Foo: Bar: Foo: Baz(3): Boo" == snapshot.get(entry_id(o))


@pytest.mark.skip("WIP")
def test_simultaneous_workflows_are_not_supported(lzy, workflow_name) -> None:
    with pytest.raises(RuntimeError) as context:
        with lzy.workflow(workflow_name, False):
            with lzy.workflow(workflow_name, False):
                pass
        assert "Simultaneous workflows are not supported" in str(context.exception)


def test_exception(lzy, workflow_name) -> None:
    @op
    def exception() -> None:
        raise TypeError("exception")

    with pytest.raises(TypeError):
        with lzy.workflow(workflow_name):
            exception()
