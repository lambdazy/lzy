import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Optional, Union, Sequence, Iterable


@dataclasses.dataclass
class AzureCredentials:
    connection_string: str


@dataclasses.dataclass
class AzureSasCredentials:
    endpoint: str
    signature: str


@dataclasses.dataclass
class AmazonCredentials:
    endpoint: str
    access_token: str
    secret_token: str


StorageCredentials = Union[
    AzureCredentials,
    AmazonCredentials,
    AzureSasCredentials,
]


@dataclass
class StorageConfig:
    credentials: StorageCredentials
    bucket: str

    @staticmethod
    def yc_object_storage(
        bucket: str, access_token: str, secret_token: str
    ) -> "StorageConfig":
        return StorageConfig(
            AmazonCredentials(
                "https://storage.yandexcloud.net", access_token, secret_token
            ),
            bucket,
        )

    @staticmethod
    def azure_blob_storage(container: str, connection_string: str) -> "StorageConfig":
        return StorageConfig(AzureCredentials(connection_string), container)

    @staticmethod
    def azure_blob_storage_sas(
        container: str, endpoint: str, signature: str
    ) -> "StorageConfig":
        return StorageConfig(AzureSasCredentials(endpoint, signature), container)


class AsyncStorageClient(ABC):
    @abstractmethod
    async def read(self, uri: str, dest: BinaryIO) -> None:
        pass

    @abstractmethod
    async def write(self, uri: str, data: BinaryIO):
        pass

    @abstractmethod
    async def blob_exists(self, uri: str) -> bool:
        pass

    @abstractmethod
    def generate_uri(self, container: str, blob: str) -> str:
        pass

    @abstractmethod
    async def sign_storage_uri(self, uri: str) -> str:
        pass


class StorageRegistry:
    @abstractmethod
    def register_storage(
        self, name: str, storage: StorageConfig, default: bool = False
    ) -> None:
        pass

    @abstractmethod
    def unregister_storage(self, name: str) -> None:
        pass

    @abstractmethod
    def config(self, storage_name: str) -> Optional[StorageConfig]:
        pass

    @abstractmethod
    def default_config(self) -> Optional[StorageConfig]:
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
