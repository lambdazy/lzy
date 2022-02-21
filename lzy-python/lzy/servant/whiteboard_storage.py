import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Any, Type

from azure.storage.blob import BlobServiceClient

from lzy.api.storage.storage_client import AzureClient, AmazonClient
from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials, AzureSasCredentials
from pure_protobuf.dataclasses_ import loads, load  # type: ignore
import cloudpickle
from lzy.api.whiteboard import check_message_field

T = TypeVar("T")  # pylint: disable=invalid-name

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

class FieldDeserializer(ABC):
    def deserialize_field(self, obj_type: Type[T], data):
        if check_message_field(obj_type):
            return loads(obj_type, data)
        return cloudpickle.loads(data)


class WhiteboardStorage(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str, obj_type: Type[T]) -> Any:
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
        self.__deserializer = FieldDeserializer()

    def read(self, url: str, obj_type: Type[T]) -> Any:
        return self.__deserializer.deserialize_field(obj_type, self.client.read(url))

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
        self.__deserializer = FieldDeserializer()

    def read(self, url: str, obj_type: Type[T]) -> Any:
        return self.__deserializer.deserialize_field(obj_type, self.client.read(url))
