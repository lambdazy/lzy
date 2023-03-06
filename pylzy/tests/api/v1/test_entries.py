from typing import cast
from unittest import TestCase

from lzy.api.v1 import Lzy, op
from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock, EnvProviderMock, StorageClientMock


# noinspection PyUnusedLocal
@op(cache=True)
def accept_int_first(i: int) -> None:
    pass


# noinspection PyUnusedLocal
@op(cache=True)
def accept_int_second(i: int) -> None:
    pass


@op(cache=True)
def foo_simple(name: str, param: int) -> str:
    return f"{name}: {param}"


@op(cache=True)
def foo_with_args(*args) -> str:
    return ', '.join(args)


@op(cache=True)
def foo_with_kwargs(*args, **kwargs) -> str:
    args_str: str = ', '.join(map(lambda i, arg: f"{i}: {arg}", enumerate(args)))
    kwargs_str: str = ', '.join(map(lambda name, arg: f"{name}: {arg}", kwargs.items()))
    return ', '.join([args_str, kwargs_str])


@op(cache=True)
def foo_with_print(name: str, value: int) -> str:
    print(f"foo was called")
    return f"{name} is {value}"


@op(cache=False)
def bar_with_print(message: str) -> str:
    print(f"bar was called")
    return f"message from bar: {message}"


class LzyEntriesTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock(), py_env_provider=EnvProviderMock())

    def test_same_args_have_same_entry_id(self):
        num = 42

        with self.lzy.workflow("test") as wf:
            accept_int_first(num)
            accept_int_first(num)
            accept_int_second(num)

        # noinspection PyUnresolvedReferences
        entry_id_1 = wf.owner.runtime.calls[0].arg_entry_ids[0]
        # noinspection PyUnresolvedReferences
        entry_id_2 = wf.owner.runtime.calls[1].arg_entry_ids[0]
        # noinspection PyUnresolvedReferences
        entry_id_3 = wf.owner.runtime.calls[2].arg_entry_ids[0]

        self.assertEqual(entry_id_1, entry_id_2)
        self.assertEqual(entry_id_1, entry_id_3)

    def test_same_local_data_stored_once(self):
        weight = 42

        self.lzy.auth(user="igor", key_path="")

        with self.lzy.workflow("test") as exec_1:
            accept_int_first(weight)
            accept_int_first(weight)
            accept_int_second(weight)

        # noinspection PyUnresolvedReferences
        uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_3 = exec_1.snapshot.get(exec_1.owner.runtime.calls[2].arg_entry_ids[0]).storage_uri

        with self.lzy.workflow("test") as exec_2:
            accept_int_second(weight)

        # noinspection PyUnresolvedReferences
        uri_4 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertEqual(uri_2, uri_3)
        self.assertEqual(uri_3, uri_4)

        storage_client = cast(StorageClientMock, self.lzy.storage_client)
        self.assertEqual(1, storage_client.store_counts[uri_1])

    def test_uris_gen_with_diff_users(self):
        weight = 42

        self.lzy.auth(user="artem", key_path="")
        with self.lzy.workflow("test") as exec_1:
            accept_int_second(weight)
        # noinspection PyUnresolvedReferences
        arg_uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        res_uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.lzy.auth(user="sergey", key_path="")
        with self.lzy.workflow("test") as exec_2:
            accept_int_second(weight)
        # noinspection PyUnresolvedReferences
        arg_uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        res_uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.lzy.auth(user="artem", key_path="")
        with self.lzy.workflow("test") as exec_3:
            accept_int_second(weight)
        # noinspection PyUnresolvedReferences
        arg_uri_3 = exec_3.snapshot.get(exec_3.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        res_uri_3 = exec_3.snapshot.get(exec_3.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.assertNotEqual(arg_uri_1, arg_uri_2)
        self.assertNotEqual(res_uri_1, res_uri_2)
        self.assertEqual(arg_uri_1, arg_uri_3)
        self.assertEqual(res_uri_1, res_uri_3)

    def test_uris_gen_with_simple_op(self):
        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_simple(n, p)
            foo_simple('length', 42)
        # noinspection PyUnresolvedReferences
        ruri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo_simple(n, p)
        # noinspection PyUnresolvedReferences
        ruri_3 = another_exec.snapshot.get(another_exec.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.assertEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_1, ruri_3)

    def test_uris_gen_with_vararg(self):
        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_with_args(n, p)
            foo_with_args(n, p)
            foo_with_args(p, n)
            foo_with_args(n, p, 'Hello, world!')

        # noinspection PyUnresolvedReferences
        ruri_0 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[2].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_3 = test_1.snapshot.get(test_1.owner.runtime.calls[3].entry_ids[0]).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo_with_args(n, p)
        # noinspection PyUnresolvedReferences
        ruri_4 = another_exec.snapshot.get(another_exec.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.assertEqual(ruri_0, ruri_1)
        self.assertNotEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_2, ruri_3)
        self.assertNotEqual(ruri_3, ruri_1)
        self.assertNotEqual(ruri_4, ruri_1)
        self.assertNotEqual(ruri_4, ruri_2)
        self.assertNotEqual(ruri_4, ruri_3)

    def test_uris_gen_with_kwargs(self):
        n: int = 13
        k: int = 42
        uris = []

        with self.lzy.workflow("test") as test_0:
            foo_with_kwargs(n, k)
            foo_with_kwargs(name=n, param=k)
            foo_with_kwargs(param=n, name=k)
            foo_with_kwargs(n, param=k)
            foo_with_kwargs(n, name=k)
        # noinspection PyUnresolvedReferences
        uris.append(test_0.snapshot.get(test_0.owner.runtime.calls[0].entry_ids[0]).storage_uri)
        # noinspection PyUnresolvedReferences
        uris.append(test_0.snapshot.get(test_0.owner.runtime.calls[1].entry_ids[0]).storage_uri)
        # noinspection PyUnresolvedReferences
        uris.append(test_0.snapshot.get(test_0.owner.runtime.calls[2].entry_ids[0]).storage_uri)
        # noinspection PyUnresolvedReferences
        uris.append(test_0.snapshot.get(test_0.owner.runtime.calls[3].entry_ids[0]).storage_uri)
        # noinspection PyUnresolvedReferences
        uris.append(test_0.snapshot.get(test_0.owner.runtime.calls[4].entry_ids[0]).storage_uri)

        for i, lhs in enumerate(uris):
            for j, rhs in enumerate(uris):
                if i != j:
                    self.assertNotEqual(lhs, rhs, msg=f"uri_{i} should not be equal uri_{j}")

        with self.lzy.workflow("test") as test_1:
            foo_with_kwargs(name=k, param=n)
        # noinspection PyUnresolvedReferences
        uris.append(test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri)

        self.assertEqual(uris[2], uris[5])

    def test_gen_uris_with_shared_ops(self):
        n: int = 13
        k: int = 42

        with self.lzy.workflow("test") as test_0:
            foo_with_args(n, k)
            foo_with_kwargs(kwarg_1=n, kwarg_2=k)
        # noinspection PyUnresolvedReferences
        ruri_0 = test_0.snapshot.get(test_0.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = test_0.snapshot.get(test_0.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow("test") as test_1:
            a = n
            b = k
            foo_with_args(a, b)
            foo_with_kwargs(kwarg_2=b, kwarg_1=a)
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_3 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        self.assertEqual(ruri_0, ruri_2)
        self.assertEqual(ruri_1, ruri_3)

    def test_diff_ops(self):
        wf_name = "wf"
        n = "number"
        v = 42

        with self.lzy.workflow(wf_name) as exec_1:
            bar_with_print(foo_with_print(n, v))
        # noinspection PyUnresolvedReferences
        uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow(name=wf_name) as exec_2:
            bar_with_print(foo_with_print(n, v))
        # noinspection PyUnresolvedReferences
        uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[1].entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertNotEqual(ruri_1, ruri_2)
