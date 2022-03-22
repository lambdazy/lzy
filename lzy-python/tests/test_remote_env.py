import multiprocessing
import pathlib
import sys
import unittest
import uuid
from typing import Any, BinaryIO
from unittest import TestCase

import cloudpickle

from lzy.api import LzyRemoteEnv
from lzy.api.storage.storage_client import StorageClient
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClientMock


class MockStorageClient(StorageClient):
    def read_to_file(self, url: str, path: str):
        pass

    def __init__(self):
        super().__init__()
        self._storage = {}

    def read(self, url: str) -> Any:
        return self._storage[url]

    def write(self, container: str, blob: str, data: BinaryIO):
        uri = container + "/" + blob
        self._storage[uri] = data
        return uri


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
        WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())
        BashServantClient.instance = lambda s, x: ServantClientMock()
        self._workflow = LzyRemoteEnv().workflow(name=WORKFLOW_NAME)
        self._storage_client = MockStorageClient()
        self._workflow._storage_client = self._storage_client

    @unittest.skip("Not used now")
    def test_py_env(self):
        multiprocessing.set_start_method('spawn')
        # Arrange
        from test_modules.level1.level1 import Level1  # type: ignore
        level1 = Level1()
        py_env = self._workflow.py_env({
            'level1': level1
        })
        manager = multiprocessing.Manager()
        result = manager.dict()
        for k, v in py_env.local_modules_uploaded():
            result[k] = self._storage_client.read(v)
        result['object'] = cloudpickle.dumps(level1)

        # Act
        process = multiprocessing.Process(target=worker, args=(result,))
        process.start()
        process.join()

        # Assert
        self.assertEqual(level1.echo(), result['result'])
