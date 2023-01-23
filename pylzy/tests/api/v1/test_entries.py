from unittest import TestCase

from api.v1.mocks import RuntimeMock, StorageRegistryMock, EnvProviderMock
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
