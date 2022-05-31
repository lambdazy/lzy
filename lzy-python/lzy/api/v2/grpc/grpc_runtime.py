import tempfile
import uuid
from typing import Callable, Any

from lzy.api.v2.api.graph import Graph
from lzy.api.v2.api.runtime.runtime import Runtime
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.api.v2.utils import is_lazy_proxy
from lzy.serialization.serializer import Serializer
from lzy.storage.credentials import StorageCredentials
from lzy.storage.storage_client import StorageClient, from_credentials


class GrpcRuntime(Runtime):
    def __init__(self, storage_client: StorageClient, bucket: str):
        self._storage_client = storage_client
        self._bucket = bucket

    @classmethod
    def from_credentials(cls, credentials: StorageCredentials, bucket: str) -> 'GrpcRuntime':
        return cls(from_credentials(credentials), bucket)

    def _load_arg(self, entry_id: str, data: Any, serializer: Serializer):
        with tempfile.NamedTemporaryFile('wb', delete=True) as write_file:
            serializer.serialize_to_file(data, write_file)
            write_file.flush()
            with open(write_file.name, 'rb') as read_file:
                read_file.seek(0)
                uri = self._storage_client.write(self._bucket, entry_id, read_file)
                # TODO: make a call to snapshot component to store entry_id and uri

    def _load_args(self, graph: Graph, serializer: Serializer):
        for call in graph.calls():
            for name, arg in call.named_arguments():
                if not is_lazy_proxy(arg):
                    # TODO: make a call to snapshot component to get entry_id for this argument
                    #  instead of manually generating it
                    entry_id = str(uuid.uuid4())
                    self._load_arg(entry_id, arg, serializer)

    def exec(self, graph: Graph, snapshot: Snapshot, progress: Callable[[], None]) -> None:
        pass

    def destroy(self) -> None:
        pass
