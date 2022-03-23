import pathlib
import sys
import uuid
from typing import Any, BinaryIO
from unittest import TestCase

import cloudpickle

from lzy.api import LzyRemoteEnv
from lzy.api.storage.storage_client import StorageClient
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClientMock

from lzy.api.whiteboard.model import (
    InMemWhiteboardApi,
    InMemSnapshotApi
)


class MockStorageClient(StorageClient):
    def read_to_file(self, url: str, path: str):
        pass

    def blob_exists(self, container: str, blob: str) -> bool:
        pass

    def __init__(self):
        super().__init__()
        self._storage = {}

    def read(self, url: str) -> Any:
        return self._storage[url]

    def write(self, container: str, blob: str, data: BinaryIO):
        uri = self.generate_uri(container, blob)
        self._storage[uri] = data
        return uri

    def generate_uri(self, container: str, blob: str) -> str:
        return container + "/" + blob


def worker(shared):
    cur_path = str(pathlib.Path().resolve())
    root_path = str(pathlib.Path("../").resolve())
    sys.path = [path for path in sys.path if path != cur_path and path != root_path]

    for k, v in shared.items():
        sys.modules[k] = cloudpickle.loads(v)
    obj = cloudpickle.loads(shared['object'])
    shared['result'] = obj.echo()


class ModulesSearchTests(TestCase):
    def setUp(self):
        self._WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())
        BashServantClient.instance = lambda s, x: ServantClientMock()
        self._env = LzyRemoteEnv()
        self._env._whiteboard_api = InMemWhiteboardApi()
        self._env._snapshot_api = InMemSnapshotApi()
        self._storage_client = MockStorageClient()

    def test_py_env_modules_selected(self):
        self._workflow = self._env.workflow(name=self._WORKFLOW_NAME)
        self._workflow._storage_client = self._storage_client
        from test_modules.level1.level1 import Level1  # type: ignore
        level1 = Level1()
        py_env = self._workflow.py_env({
            'level1': level1
        })
        result = dict()
        for k, v in py_env.local_modules_uploaded():
            result[k] = self._storage_client.read(v)
        self.assertEqual(len(result), 1)
        self.assertTrue('test_modules' in result)

    def test_py_env_modules_user_provided(self):
        self._workflow = self._env.workflow(name=self._WORKFLOW_NAME, local_module_paths=['test_modules_2'])
        self._workflow._storage_client = self._storage_client
        from test_modules.level1.level1 import Level1  # type: ignore
        level1 = Level1()
        py_env = self._workflow.py_env({
            'level1': level1
        })
        result = dict()
        for k, v in py_env.local_modules_uploaded():
            result[k] = self._storage_client.read(v)
        self.assertEqual(len(result), 1)
        self.assertTrue('test_modules_2' in result)
        self.assertFalse('test_modules' in result)
