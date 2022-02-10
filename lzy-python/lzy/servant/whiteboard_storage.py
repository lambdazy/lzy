import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Any

from azure.storage.blob import BlobServiceClient

from lzy.api.storage.storage_client import AzureClient, AmazonClient
from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials, AzureSasCredentials

T = TypeVar("T")  # pylint: disable=invalid-name

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


class WhiteboardStorage(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str) -> Any:
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

    def read(self, url: str) -> Any:
        return self.client.read(url)

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

    def read(self, url: str) -> Any:
        return self.client.read(url)
