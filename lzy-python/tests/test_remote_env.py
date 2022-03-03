import multiprocessing
import pathlib
import sys
import unittest
import uuid
from pathlib import Path
from typing import Any, Optional, Mapping, BinaryIO
from unittest import TestCase

import cloudpickle

from lzy.api import LzyRemoteEnv
from lzy.api.storage.storage_client import StorageClient
from lzy.api.whiteboard.credentials import StorageCredentials, AmazonCredentials
from lzy.model.channel import Bindings, Channel
from lzy.model.slot import Slot
from lzy.model.zygote import Zygote
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient, Execution, CredentialsTypes


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


class ServantClientMock(ServantClient):
    def mount(self) -> Path:
        pass

    def get_slot_path(self, slot: Slot) -> Path:
        pass

    def create_channel(self, channel: Channel):
        pass

    def destroy_channel(self, channel: Channel):
        pass

    def touch(self, slot: Slot, channel: Channel):
        pass

    def publish(self, zygote: Zygote):
        pass

    def run(self, execution_id: str, zygote: Zygote, bindings: Bindings) -> Execution:
        pass

    def get_credentials(self, typ: CredentialsTypes, bucket: str) -> StorageCredentials:
        return AmazonCredentials("", "", "")

    def get_bucket(self) -> str:
        return "bucket"


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
        from tests.test_modules.level1.level1 import Level1  # type: ignore
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
