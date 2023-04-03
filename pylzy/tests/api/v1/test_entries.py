import asyncio
import tempfile
from typing import cast, List
from unittest import TestCase

from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from lzy.api.v1 import Lzy, op, LocalRuntime
from lzy.storage.api import Storage, S3Credentials
from lzy.storage.registry import DefaultStorageRegistry
from lzy.types import File
from tests.api.v1.mocks import EnvProviderMock, StorageClientMock, RuntimeMock, StorageRegistryMock
from tests.api.v1.utils import create_bucket

# Catboost requires both pandas & numpy
# noinspection PyPackageRequirements
import pandas as pd
# noinspection PyPackageRequirements
import numpy as np


# noinspection PyUnusedLocal
@op(cache=True)
def accept_df(frame: pd.DataFrame) -> None:
    pass


# noinspection PyUnusedLocal
@op(cache=True)
def accept_file(script: File) -> None:
    pass


@op(cache=True)
def foo(name: str, param: int) -> str:
    return f"{name}: {param}"


@op(cache=True)
def foo_varargs(*args) -> str:
    return ', '.join(map(repr, args))


@op(cache=True)
def foo_kwargs(*args, **kwargs) -> str:
    args_str: str = ', '.join([f"{i}: {arg}" for i, arg in enumerate(args)])
    kwargs_str: str = ', '.join([f"{name}: {arg}" for name, arg in kwargs.items()])
    return ', '.join([args_str, kwargs_str])


@op(cache=False)
def bar(message: str) -> str:
    print(f"bar was called")
    return f"message from bar: {message}"


@op(cache=True)
def accept_arr(ints: List[int]) -> List[int]:
    return ints


@op(cache=True)
def accept_arr_and_modify(ints: List[int]) -> List[int]:
    ints.append(len(ints))
    return ints


class LzyEntriesTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock(), py_env_provider=EnvProviderMock())

    def test_same_op_diff_entries(self):
        num = 42

        with self.lzy.workflow("test") as wf:
            foo_varargs(num)
            foo_varargs(num)
            foo_varargs(13)

        # noinspection PyUnresolvedReferences
        entry_id_1 = wf.owner.runtime.calls[0].entry_ids[0]
        # noinspection PyUnresolvedReferences
        entry_id_2 = wf.owner.runtime.calls[1].entry_ids[0]
        # noinspection PyUnresolvedReferences
        entry_id_3 = wf.owner.runtime.calls[2].entry_ids[0]

        self.assertNotEqual(entry_id_1, entry_id_2)
        self.assertNotEqual(entry_id_2, entry_id_3)
        self.assertNotEqual(entry_id_3, entry_id_1)

    def test_same_local_data_stored_once(self):
        weight = 42
        param = "hehe"

        self.lzy.auth(user="igor", key_path="")

        with self.lzy.workflow("test") as exec_1:
            foo(param, weight)
            foo_varargs(weight)

        # noinspection PyUnresolvedReferences
        uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].arg_entry_ids[1]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri

        with self.lzy.workflow("test") as exec_2:
            foo_varargs(weight)

        # noinspection PyUnresolvedReferences
        uri_4 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertEqual(uri_2, uri_4)

        storage_client = cast(StorageClientMock, self.lzy.storage_client)
        self.assertEqual(1, storage_client.store_counts[uri_1])

    def test_uris_gen_for_repeating_arg(self):
        n: str = 'length'

        with self.lzy.workflow("test") as test_1:
            foo_varargs(n, 'length', n)

        # noinspection PyUnresolvedReferences
        uri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[0].arg_entry_ids[1]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_3 = test_1.snapshot.get(test_1.owner.runtime.calls[0].arg_entry_ids[2]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertEqual(uri_2, uri_3)

    def test_uris_gen_with_diff_users(self):
        weight = 42

        self.lzy.auth(user="artem", key_path="")
        with self.lzy.workflow("test") as exec_1:
            foo_varargs(weight)
        # noinspection PyUnresolvedReferences
        arg_uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        res_uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.lzy.auth(user="sergey", key_path="")
        with self.lzy.workflow("test") as exec_2:
            foo_varargs(weight)
        # noinspection PyUnresolvedReferences
        arg_uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        res_uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.lzy.auth(user="artem", key_path="")
        with self.lzy.workflow("test") as exec_3:
            foo_varargs(weight)
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
            foo(n, p)
            foo('length', 42)
        # noinspection PyUnresolvedReferences
        ruri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo(n, p)
        # noinspection PyUnresolvedReferences
        ruri_3 = another_exec.snapshot.get(another_exec.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.assertEqual(ruri_1, ruri_2)
        self.assertNotEqual(ruri_1, ruri_3)

    def test_uris_gen_with_vararg(self):
        n: str = 'length'
        p: int = 42

        with self.lzy.workflow("test") as test_1:
            foo_varargs(n, p)
            foo_varargs(n, p)
            foo_varargs(p, n)
            foo_varargs(n, p, 'Hello, world!')

        # noinspection PyUnresolvedReferences
        ruri_0 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[2].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_3 = test_1.snapshot.get(test_1.owner.runtime.calls[3].entry_ids[0]).storage_uri

        with self.lzy.workflow("another") as another_exec:
            foo_varargs(n, p)
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
            foo_kwargs(n, k)
            foo_kwargs(name=n, param=k)
            foo_kwargs(param=n, name=k)
            foo_kwargs(n, param=k)
            foo_kwargs(n, name=k)
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
            foo_kwargs(name=k, param=n)
        # noinspection PyUnresolvedReferences
        uris.append(test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri)

        self.assertEqual(uris[2], uris[5])

    def test_gen_uris_with_shared_ops(self):
        n: int = 13
        k: int = 42

        with self.lzy.workflow("test") as test_0:
            foo_varargs(n, k)
            foo_kwargs(kwarg_1=n, kwarg_2=k)
        # noinspection PyUnresolvedReferences
        ruri_0 = test_0.snapshot.get(test_0.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = test_0.snapshot.get(test_0.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow("test") as test_1:
            a = n
            b = k
            foo_varargs(a, b)
            foo_kwargs(kwarg_2=b, kwarg_1=a)
        # noinspection PyUnresolvedReferences
        ruri_2 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_3 = test_1.snapshot.get(test_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        self.assertEqual(ruri_0, ruri_2)
        self.assertEqual(ruri_1, ruri_3)

    def test_diff_ops(self):
        wf_name = "wf"
        v = 42

        with self.lzy.workflow(wf_name) as exec_1:
            bar(foo_varargs(v))
        # noinspection PyUnresolvedReferences
        uri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_1 = exec_1.snapshot.get(exec_1.owner.runtime.calls[1].entry_ids[0]).storage_uri

        with self.lzy.workflow(name=wf_name) as exec_2:
            bar(foo_varargs(v))
        # noinspection PyUnresolvedReferences
        uri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[1].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        ruri_2 = exec_2.snapshot.get(exec_2.owner.runtime.calls[1].entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertNotEqual(ruri_1, ruri_2)

    def test_op_versions_changes(self):
        # noinspection PyUnusedLocal
        @op(cache=True, version="1.0")
        def first_op(name: str) -> None:
            pass

        with self.lzy.workflow("test") as test_1:
            first_op("length")

        # noinspection PyUnresolvedReferences
        ruri_1 = test_1.snapshot.get(test_1.owner.runtime.calls[0].entry_ids[0]).storage_uri

        # noinspection PyUnusedLocal
        @op(cache=True, version="1.1")
        def first_op(name: str) -> None:
            pass

        with self.lzy.workflow("test") as test_2:
            first_op("length")

        # noinspection PyUnresolvedReferences
        ruri_2 = test_2.snapshot.get(test_2.owner.runtime.calls[0].entry_ids[0]).storage_uri

        self.assertNotEqual(ruri_1, ruri_2)

    def test_local_data_changed_by_ref(self):
        with self.lzy.workflow("wf") as wf:
            ints = []
            for i in range(5):
                ints.append(i)
                accept_arr(ints)

        snapshot = wf.snapshot
        runtime = wf.owner.runtime

        arg_uris = []
        ret_uris = []
        for i in range(5):
            # noinspection PyUnresolvedReferences
            arg_uris.append(snapshot.get(runtime.calls[i].arg_entry_ids[0]).storage_uri)
            # noinspection PyUnresolvedReferences
            ret_uris.append(snapshot.get(runtime.calls[i].entry_ids[0]).storage_uri)

        for i, lhs in enumerate(arg_uris):
            for j, rhs in enumerate(arg_uris):
                if i != j:
                    self.assertNotEqual(lhs, rhs, msg=f"arg_uri_{i} should not be equal arg_uri_{j}")

        for i, lhs in enumerate(ret_uris):
            for j, rhs in enumerate(ret_uris):
                if i != j:
                    self.assertNotEqual(lhs, rhs, msg=f"ret_uri_{i} should not be equal ret_uri_{j}")

    def test_local_data_can_not_be_changed_by_ref_inside_op(self):
        with self.lzy.workflow("wf") as wf:
            ints = []
            for i in range(5):
                accept_arr_and_modify(ints)

        snapshot = wf.snapshot
        runtime = wf.owner.runtime

        arg_uris = []
        ret_uris = []
        for i in range(5):
            # noinspection PyUnresolvedReferences
            arg_uris.append(snapshot.get(runtime.calls[i].arg_entry_ids[0]).storage_uri)
            # noinspection PyUnresolvedReferences
            ret_uris.append(snapshot.get(runtime.calls[i].entry_ids[0]).storage_uri)

        for i, lhs in enumerate(arg_uris):
            for j, rhs in enumerate(arg_uris):
                if i != j:
                    self.assertEqual(lhs, rhs, msg=f"arg_uri_{i} should be equal arg_uri_{j}")

        for i, lhs in enumerate(ret_uris):
            for j, rhs in enumerate(ret_uris):
                if i != j:
                    self.assertEqual(lhs, rhs, msg=f"ret_uri_{i} should be equal ret_uri_{j}")

    def test_op_passed_as_arg_to_itself(self):
        with self.lzy.workflow("wf") as wf:
            a = [1, 2, 3, 4, 5]
            accept_arr(accept_arr(a))
            accept_arr(accept_arr(a))

        snapshot = wf.snapshot
        runtime = wf.owner.runtime

        # noinspection PyUnresolvedReferences
        entry_1 = snapshot.get(runtime.calls[0].entry_ids[0])
        # noinspection PyUnresolvedReferences
        entry_2 = snapshot.get(runtime.calls[1].entry_ids[0])

        # noinspection PyUnresolvedReferences
        entry_3 = snapshot.get(runtime.calls[2].entry_ids[0])
        # noinspection PyUnresolvedReferences
        entry_4 = snapshot.get(runtime.calls[3].entry_ids[0])

        self.assertEqual(entry_1.storage_uri, entry_3.storage_uri)
        self.assertEqual(entry_2.storage_uri, entry_4.storage_uri)
        self.assertNotEqual(entry_1.storage_uri, entry_2.storage_uri)

    def test_cached_op_with_some_as_arg(self):
        df = pd.DataFrame(np.random.choice(['lzy', 'yzl', 'zly'], size=(500, 3)))

        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write("Hello, world!\n".encode("utf-8"))
            with self.lzy.workflow("wf") as wf:
                accept_df(df)
                accept_df(df)
                accept_file(File(tmp.name))
                accept_file(File(tmp.name))

        snapshot = wf.snapshot
        runtime = wf.owner.runtime

        # noinspection PyUnresolvedReferences
        uri_1 = snapshot.get(runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = snapshot.get(runtime.calls[1].arg_entry_ids[0]).storage_uri

        # noinspection PyUnresolvedReferences
        uri_3 = snapshot.get(runtime.calls[2].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_4 = snapshot.get(runtime.calls[3].arg_entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)
        self.assertEqual(uri_3, uri_4)

    def test_cached_op_with_empty_file(self):
        with tempfile.NamedTemporaryFile() as tmp:
            with self.lzy.workflow("wf") as wf:
                accept_file(File(tmp.name))
                accept_file(File(tmp.name))

        snapshot = wf.snapshot
        runtime = wf.owner.runtime

        # noinspection PyUnresolvedReferences
        uri_1 = snapshot.get(runtime.calls[0].arg_entry_ids[0]).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = snapshot.get(runtime.calls[1].arg_entry_ids[0]).storage_uri

        self.assertEqual(uri_1, uri_2)


class LzyEntriesTestsWithLocalRuntime(TestCase):
    endpoint_url = None
    s3_service = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.s3_service = ThreadedMotoServer(port=12344)
        cls.s3_service.start()
        cls.endpoint_url = "http://localhost:12344"
        asyncio.run(create_bucket(cls.endpoint_url))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.s3_service.stop()

    def setUp(self):
        self.lzy = Lzy(runtime=LocalRuntime(), py_env_provider=EnvProviderMock(),
                       storage_registry=DefaultStorageRegistry())
        self.storage_uri = "s3://bucket/prefix"
        storage_config = Storage(uri=self.storage_uri,
                                 credentials=S3Credentials(self.endpoint_url, access_key_id="", secret_access_key=""))
        self.lzy.storage_registry.register_storage("default", storage_config, default=True)

    def test_local_data_and_op_result_is_not_same_input(self):
        with self.lzy.workflow("wf") as wf:
            ints: List[int] = [1, 2, 3]
            same_ints: List[int] = accept_arr(ints)
            accept_arr(same_ints)

            eid_1 = wf.call_queue[0].entry_ids[0]
            eid_2 = wf.call_queue[1].entry_ids[0]

        snapshot = wf.snapshot

        # noinspection PyUnresolvedReferences
        uri_1 = snapshot.get(eid_1).storage_uri
        # noinspection PyUnresolvedReferences
        uri_2 = snapshot.get(eid_2).storage_uri

        self.assertEqual(ints, same_ints)
        self.assertNotEqual(uri_1, uri_2)

    def test_op_result_changed_by_ref(self):
        arg_eids = []
        ret_eids = []

        with self.lzy.workflow("wf") as wf:
            ints = accept_arr([])
            for i in range(5):
                ints.append(i)
                accept_arr(ints)

                arg_eids.append(wf.call_queue[i].arg_entry_ids[0])
                ret_eids.append(wf.call_queue[i].entry_ids[0])

        snapshot = wf.snapshot

        for i, lhs in enumerate(arg_eids):
            for j, rhs in enumerate(arg_eids):
                if i != j:
                    luri = snapshot.get(lhs).storage_uri
                    ruri = snapshot.get(rhs).storage_uri
                    self.assertNotEqual(luri, ruri, msg=f"arg_uri_{i} should not be equal arg_uri_{j}")

        for i, lhs in enumerate(ret_eids):
            for j, rhs in enumerate(ret_eids):
                if i != j:
                    luri = snapshot.get(lhs).storage_uri
                    ruri = snapshot.get(rhs).storage_uri
                    self.assertNotEqual(luri, ruri, msg=f"ret_uri_{i} should not be equal ret_uri_{j}")

    def test_op_result_can_not_be_changed_by_ref_inside_op(self):
        arg_eids = []
        ret_eids = []

        with self.lzy.workflow("wf") as wf:
            ints = accept_arr([])
            for i in range(5):
                accept_arr_and_modify(ints)

                arg_eids.append(wf.call_queue[i + 1].arg_entry_ids[0])
                ret_eids.append(wf.call_queue[i + 1].entry_ids[0])

        snapshot = wf.snapshot

        for i, lhs in enumerate(arg_eids):
            for j, rhs in enumerate(arg_eids):
                if i != j:
                    luri = snapshot.get(lhs).storage_uri
                    ruri = snapshot.get(rhs).storage_uri
                    self.assertEqual(luri, ruri, msg=f"arg_uri_{i} should be equal arg_uri_{j}")

        for i, lhs in enumerate(ret_eids):
            for j, rhs in enumerate(ret_eids):
                if i != j:
                    luri = snapshot.get(lhs).storage_uri
                    ruri = snapshot.get(rhs).storage_uri
                    self.assertEqual(luri, ruri, msg=f"ret_uri_{i} should be equal ret_uri_{j}")
