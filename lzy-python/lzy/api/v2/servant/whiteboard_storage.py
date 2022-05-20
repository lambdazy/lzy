import logging
from abc import ABC, abstractmethod
from typing import TypeVar, IO

from azure.storage.blob import BlobServiceClient
from pure_protobuf.dataclasses_ import loads, load  # type: ignore

from lzy.storage.credentials import StorageCredentials, AmazonCredentials, AzureCredentials, AzureSasCredentials
from lzy.storage.storage_client import AzureClient, AmazonClient

T = TypeVar("T")  # pylint: disable=invalid-name

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


class WhiteboardStorage(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str, dest: IO) -> None:
        pass

    @staticmethod
    def create(credentials: StorageCredentials) -> "WhiteboardStorage":
        if isinstance(credentials, AmazonCredentials):
            return AmazonWhiteboardStorage(credentials)
        if isinstance(credentials, AzureCredentials):
            return AzureWhiteboardStorage.from_connection_string(credentials)
        return AzureWhiteboardStorage.from_sas(credentials)


class AzureWhiteboardStorage(WhiteboardStorage):
    def __init__(self, client: BlobServiceClient):
        super().__init__()
        self.client: AzureClient = AzureClient(client)

    def read(self, url: str, dest: IO) -> None:
        self.client.read(url, dest)

    @staticmethod
    def from_connection_string(credentials: AzureCredentials) -> 'AzureWhiteboardStorage':
        return AzureWhiteboardStorage(BlobServiceClient.from_connection_string(credentials.connection_string))

    @staticmethod
    def from_sas(credentials: AzureSasCredentials) -> 'AzureWhiteboardStorage':
        return AzureWhiteboardStorage(BlobServiceClient(credentials.endpoint))


class AmazonWhiteboardStorage(WhiteboardStorage):
    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self.client = AmazonClient(credentials)
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    def read(self, url: str, dest: IO) -> None:
        self.client.read(url, dest)
