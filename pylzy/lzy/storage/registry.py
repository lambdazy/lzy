from collections import defaultdict
from typing import Dict, Optional, Iterable

from lzy.storage.api import AsyncStorageClient, Storage, StorageRegistry
from lzy.storage.async_ import from_credentials


class DefaultStorageRegistry(StorageRegistry):
    def __init__(self):
        self.__credentials_map: Dict[str, Optional[Storage]] = defaultdict(
            lambda: None
        )
        self.__clients_map: Dict[str, Optional[AsyncStorageClient]] = defaultdict(
            lambda: None
        )

        self.__default_name: Optional[str] = None
        self.__default_config: Optional[Storage] = None
        self.__default_client: Optional[AsyncStorageClient] = None

    def register_storage(self, name: str, storage: Storage, default: bool = False):
        self.__credentials_map[name] = storage
        client = from_credentials(storage.credentials)
        self.__clients_map[name] = client
        if default:
            self.__default_config = storage
            self.__default_name = name
            self.__default_client = client

    def unregister_storage(self, name: str):
        self.__credentials_map.pop(name)
        self.__clients_map.pop(name)
        if self.__default_name == name:
            self.__default_config = None
            self.__default_name = None
            self.__default_client = None

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
