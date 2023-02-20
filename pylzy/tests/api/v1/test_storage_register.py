import uuid
from unittest import TestCase

from api.v1.mocks import EnvProviderMock
from lzy.api.v1 import LocalRuntime, Lzy
from lzy.storage.api import Storage


class RegisterStorageTests(TestCase):
    def setUp(self):
        self.workflow_name = "workflow_" + str(uuid.uuid4())
        self.lzy = Lzy(runtime=LocalRuntime(), py_env_provider=EnvProviderMock())

    def test_register_default_storage_name(self):
        self.lzy.storage_registry.register_storage("provided_default_storage",
                                                   Storage.fs_storage("/tmp/default_lzy_storage"),
                                                   default=True)
        with self.assertRaises(ValueError):
            with self.lzy.workflow("wf"):
                pass

    def test_unregister_not_registered(self):
        with self.assertRaises(ValueError):
            self.lzy.storage_registry.unregister_storage("provided_default_storage")
