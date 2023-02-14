from typing import cast
from unittest import TestCase

from lzy.api.v1 import Lzy, op
from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock, EnvProviderMock, StorageClientMock


class LzyEntriesTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock(), py_env_provider=EnvProviderMock())

    def test_same_args_have_same_entry_id(self):
        # noinspection PyUnusedLocal
        @op
        def accept_int_first(i: int) -> None:
            pass

        # noinspection PyUnusedLocal
        @op
        def accept_int_second(i: int) -> None:
            pass

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

    def test_same_local_data_have_same_storage_uri_and_stored_once(self):
        # noinspection PyUnusedLocal
        @op
        def first_model(w: int) -> None:
            pass

        # noinspection PyUnusedLocal
        @op
        def second_model(w: int) -> None:
            pass

        weight = 42

        with self.lzy.workflow("test") as exec_1:
            first_model(weight)
            first_model(weight)
            second_model(weight)

        # noinspection PyUnresolvedReferences
        eid_1 = exec_1.owner.runtime.calls[0].arg_entry_ids[0]
        # noinspection PyUnresolvedReferences
        eid_2 = exec_1.owner.runtime.calls[1].arg_entry_ids[0]
        # noinspection PyUnresolvedReferences
        eid_3 = exec_1.owner.runtime.calls[2].arg_entry_ids[0]

        uri_1 = exec_1.snapshot.get(eid_1).storage_uri
        uri_2 = exec_1.snapshot.get(eid_2).storage_uri
        uri_3 = exec_1.snapshot.get(eid_3).storage_uri

        with self.lzy.workflow("test") as exec_2:
            second_model(weight)

        # noinspection PyUnresolvedReferences
        eid_4 = exec_2.owner.runtime.calls[0].arg_entry_ids[0]

        uri_4 = exec_2.snapshot.get(eid_4).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertEqual(uri_2, uri_3)
        self.assertEqual(uri_3, uri_4)

        storage_client = cast(StorageClientMock, self.lzy.storage_client)
        self.assertEqual(1, storage_client.write_counts[uri_1])

    def test_simple_op_uri_generation(self):
        @op
        def foo_simple(name: str, param: int) -> str:
            return f"{name}: {param}"

        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_simple(n, p)

        # noinspection PyUnresolvedReferences
        reid_1 = test_1.owner.runtime.calls[0].entry_ids[0]
        ruri_1 = test_1.snapshot.get(reid_1).storage_uri

        with self.lzy.workflow("test") as test_2:
            foo_simple('length', 42)

        # noinspection PyUnresolvedReferences
        reid_2 = test_2.owner.runtime.calls[0].entry_ids[0]
        ruri_2 = test_2.snapshot.get(reid_2).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo_simple(n, p)

        # noinspection PyUnresolvedReferences
        reid_3 = another_exec.owner.runtime.calls[0].entry_ids[0]
        ruri_3 = another_exec.snapshot.get(reid_3).storage_uri

        self.assertEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_1, ruri_3)

    def test_op_with_vararg(self):
        @op
        def foo_with_args(*args) -> str:
            return ', '.join(args)

        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_with_args(n, p)

        # noinspection PyUnresolvedReferences
        reid_1 = test_1.owner.runtime.calls[0].entry_ids[0]
        ruri_1 = test_1.snapshot.get(reid_1).storage_uri

        with self.lzy.workflow("test") as test_2:
            foo_with_args(p, n)

        # noinspection PyUnresolvedReferences
        reid_2 = test_2.owner.runtime.calls[0].entry_ids[0]
        ruri_2 = test_2.snapshot.get(reid_2).storage_uri

        with self.lzy.workflow("test") as test_3:
            foo_with_args(n, p, 'Hello, world!')

        # noinspection PyUnresolvedReferences
        reid_3 = test_3.owner.runtime.calls[0].entry_ids[0]
        ruri_3 = test_3.snapshot.get(reid_3).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo_with_args(n, p)

        # noinspection PyUnresolvedReferences
        reid_4 = another_exec.owner.runtime.calls[0].entry_ids[0]
        ruri_4 = another_exec.snapshot.get(reid_4).storage_uri

        self.assertNotEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_2, ruri_3)
        self.assertNotEqual(ruri_3, ruri_1)
        self.assertNotEqual(ruri_4, ruri_1)
        self.assertNotEqual(ruri_4, ruri_2)
        self.assertNotEqual(ruri_4, ruri_3)

    def test_op_with_kwargs(self):
        @op
        def foo_with_kwargs(*args, **kwargs) -> str:
            args_str: str = ', '.join(map(lambda i, arg: f"{i}: {arg}", enumerate(args)))
            kwargs_str: str = ', '.join(map(lambda name, arg: f"{name}: {arg}", kwargs.items()))
            return ', '.join([args_str, kwargs_str])

        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_with_kwargs(n, p)

        # noinspection PyUnresolvedReferences
        reid_1 = test_1.owner.runtime.calls[0].entry_ids[0]
        ruri_1 = test_1.snapshot.get(reid_1).storage_uri

        with self.lzy.workflow("test") as test_2:
            foo_with_kwargs(name=n, param=p)

        # noinspection PyUnresolvedReferences
        reid_2 = test_2.owner.runtime.calls[0].entry_ids[0]
        ruri_2 = test_2.snapshot.get(reid_2).storage_uri

        with self.lzy.workflow("test") as test_3:
            foo_with_kwargs(param=p, name=n)

        # noinspection PyUnresolvedReferences
        reid_3 = test_3.owner.runtime.calls[0].entry_ids[0]
        ruri_3 = test_3.snapshot.get(reid_3).storage_uri

        with self.lzy.workflow("test") as test_4:
            foo_with_kwargs(n, param=p)

        # noinspection PyUnresolvedReferences
        reid_4 = test_4.owner.runtime.calls[0].entry_ids[0]
        ruri_4 = test_4.snapshot.get(reid_4).storage_uri

        self.assertEqual(ruri_2, ruri_3)
        self.assertNotEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_1, ruri_4)
        self.assertNotEqual(ruri_4, ruri_2)
