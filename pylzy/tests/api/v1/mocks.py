from typing import List, Dict, Callable, Optional, Iterable, BinaryIO

from lzy.api.v1 import Runtime, LzyCall, LzyWorkflow
from lzy.api.v1.runtime import ProgressStep
from lzy.api.v1.whiteboards import WbRef
from lzy.storage.api import StorageRegistry, Storage, AsyncStorageClient


class RuntimeMock(Runtime):
    def __init__(self):
        self.calls: List[LzyCall] = []

    async def start(self, workflow: "LzyWorkflow"):
        pass

    async def exec(self, calls: List[LzyCall], links: Dict[str, WbRef],
                   progress: Callable[[ProgressStep], None]) -> None:
        self.calls = calls

    async def destroy(self) -> None:
        pass


class StorageClientMock(AsyncStorageClient):
    async def size_in_bytes(self, uri: str) -> int:
        pass

    async def read(self, uri: str, dest: BinaryIO, progress: Optional[Callable[[int], None]] = None) -> None:
        pass

    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int], None]] = None):
        pass

    async def blob_exists(self, uri: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass

    async def sign_storage_uri(self, uri: str) -> str:
        pass


class StorageRegistryMock(StorageRegistry):

    def register_storage(self, name: str, storage: Storage, default: bool = False) -> None:
        pass

    def unregister_storage(self, name: str) -> None:
        pass

    def config(self, storage_name: str) -> Optional[Storage]:
        pass

    def default_config(self) -> Optional[Storage]:
        return Storage.azure_blob_storage("", "")

    def default_storage_name(self) -> Optional[str]:
        return "storage_name"

    def client(self, storage_name: str) -> Optional[AsyncStorageClient]:
        pass

    def default_client(self) -> Optional[AsyncStorageClient]:
        return StorageClientMock()

    def available_storages(self) -> Iterable[str]:
        pass
