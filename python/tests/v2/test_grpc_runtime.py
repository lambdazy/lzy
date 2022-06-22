import uuid
from typing import IO
from unittest import TestCase

from lzy.api.v2.api import op
from lzy.api.v2.api.graph import Graph, GraphBuilder
from lzy.api.v2.api.lzy import Lzy
from lzy.api.v2.grpc.grpc_runtime import GrpcRuntime
from lzy.serialization.serializer import DefaultSerializer
from lzy.storage.storage_client import StorageClient


class MockStorageClient(StorageClient):
    def __init__(self):
        super().__init__()
        self.storage = {}

    def read_to_file(self, url: str, path: str):
        pass

    def blob_exists(self, container: str, blob: str) -> bool:
        uri = self.generate_uri(container, blob)
        return uri in self.storage

    def read(self, url: str, dest: IO) -> None:
        dest.write(self.storage[url])

    def write(self, container: str, blob: str, data: IO):
        uri = self.generate_uri(container, blob)
        self.storage[uri] = data.read()
        return uri

    def generate_uri(self, container: str, blob: str) -> str:
        return container + "/" + blob


@op
def foo(a: str) -> int:
    return int(a) + 10


@op
def bar(a: str, b: int) -> str:
    return a + str(b)


class GrpcRuntimeTests(TestCase):
    def setUp(self):
        self._WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())
        self._storage_client = MockStorageClient()
        self._bucket = str(uuid.uuid4())
        self._runtime = GrpcRuntime(self._storage_client, self._bucket)
        self._lzy = Lzy(runtime=self._runtime)
        self._serializer = DefaultSerializer()

    def test_argument_upload(self):
        with self._lzy.workflow(self._WORKFLOW_NAME, False) as workflow:
            f = foo("24")
            b = bar("42", f)
            graph: Graph = GraphBuilder().snapshot_id(str(uuid.uuid4())).add_call(b.lzy_call).build()
            self._runtime._load_args(graph, DefaultSerializer())
            self.assertTrue(len(self._storage_client.storage) == 2)
            values = list(map(lambda x: self._serializer.deserialize_from_string(x, str),
                              list(self._storage_client.storage.values())))
            self.assertTrue("42" in values)
            self.assertTrue("24" in values)
