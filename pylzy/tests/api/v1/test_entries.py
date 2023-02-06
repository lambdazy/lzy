from dataclasses import dataclass
from unittest import TestCase

from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock, EnvProviderMock
from lzy.api.v1 import Lzy, op


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

    def test_same_local_data_have_same_storage_uri(self):
        # @dataclass
        # class Weights:
        #     param_1: int
        #     param_2: float
        #     param_3: str

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
