from collections import defaultdict
from typing import Dict, Optional, Iterable

from lzy.storage.api import AsyncStorageClient, Storage, StorageRegistry
from lzy.storage.async_ import from_credentials


class DefaultStorageRegistry(StorageRegistry):
    def __init__(self,
                 default_storage_name: str,
                 default_storage: Optional[Storage] = None):
        self.__credentials_map: Dict[str, Optional[Storage]] = defaultdict(
            lambda: None
        )
        self.__clients_map: Dict[str, Optional[AsyncStorageClient]] = defaultdict(
            lambda: None
        )
        self.__default_name: Optional[str] = default_storage_name
        self.__default_config: Optional[Storage] = None
        self.__default_client: Optional[AsyncStorageClient] = None
        if default_storage is not None:
            self.register_default_storage(default_storage)

    def register_default_storage(self, default_storage: Storage):
        client = from_credentials(default_storage.credentials)
        self.__default_config = default_storage
        self.__default_client = client
        self.__credentials_map[self.__default_name] = default_storage
        self.__clients_map[self.__default_name] = client

    def register_storage(self, name: str, storage: Storage):
        if name == self.__default_name:
            raise ValueError(f"Storage name {name} is taken by default storage")
        self.__credentials_map[name] = storage
        client = from_credentials(storage.credentials)
        self.__clients_map[name] = client

    def unregister_storage(self, name: str):
        if name == self.__default_name:
            raise ValueError(f"Can't unregister default storage")
        self.__credentials_map.pop(name)
        self.__clients_map.pop(name)

    def config(self, storage_name: str) -> Optional[Storage]:
        return self.__credentials_map.get(storage_name)

    def default_config(self) -> Optional[Storage]:
        return self.__default_config

    def default_storage_name(self) -> Optional[str]:
        return self.__default_name

    def client(self, storage_name: str) -> Optional[AsyncStorageClient]:
        return self.__clients_map.get(storage_name)

    def default_client(self) -> Optional[AsyncStorageClient]:
        return self.__default_client

    def available_storages(self) -> Iterable[str]:
        return self.__clients_map.keys()
