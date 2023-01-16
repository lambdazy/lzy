import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Optional, Union, Iterable, Callable, Any


@dataclasses.dataclass
class AzureCredentials:
    connection_string: str


@dataclasses.dataclass
class AzureSasCredentials:
    endpoint: str
    signature: str


@dataclasses.dataclass
class S3Credentials:
    endpoint: str
    access_token: str
    secret_token: str


StorageCredentials = Union[
    AzureCredentials,
    S3Credentials,
    AzureSasCredentials,
]


@dataclass
class Storage:
    credentials: StorageCredentials
    uri: str

    @staticmethod
    def yc_object_storage(
        bucket: str, access_token: str, secret_token: str
    ) -> "Storage":
        return Storage(
            S3Credentials(
                "https://storage.yandexcloud.net", access_token, secret_token
            ),
            bucket,
        )

    @staticmethod
    def azure_blob_storage(container: str, connection_string: str) -> "Storage":
        return Storage(AzureCredentials(connection_string), container)

    @staticmethod
    def azure_blob_storage_sas(
        container: str, endpoint: str, signature: str
    ) -> "Storage":
        return Storage(AzureSasCredentials(endpoint, signature), container)


class AsyncStorageClient(ABC):
    @abstractmethod
    async def size_in_bytes(self, uri: str) -> int:
        pass

    @abstractmethod
    async def read(self, uri: str, dest: BinaryIO, progress: Optional[Callable[[int], Any]] = None) -> None:
        pass

    @abstractmethod
    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int], Any]] = None):
        pass

    @abstractmethod
    async def blob_exists(self, uri: str) -> bool:
        pass

    @abstractmethod
    async def copy(self, from_uri: str, to_uri: str) -> None:
        pass

    @abstractmethod
    async def sign_storage_uri(self, uri: str) -> str:
        pass


class StorageRegistry:
    @abstractmethod
    def register_storage(
        self, name: str, storage: Storage, default: bool = False
    ) -> None:
        pass

    @abstractmethod
    def unregister_storage(self, name: str) -> None:
        pass

    @abstractmethod
    def config(self, storage_name: str) -> Optional[Storage]:
        pass

    @abstractmethod
    def default_config(self) -> Optional[Storage]:
        pass

    @abstractmethod
    def default_storage_name(self) -> Optional[str]:
        pass

    @abstractmethod
    def client(self, storage_name: str) -> Optional[AsyncStorageClient]:
        pass

    @abstractmethod
    def default_client(self) -> Optional[AsyncStorageClient]:
        pass

    @abstractmethod
    def available_storages(self) -> Iterable[str]:
        pass
