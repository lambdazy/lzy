import logging
import pathlib
from abc import ABC, abstractmethod
from typing import TypeVar, Any
from urllib import parse

from azure.storage.blob import BlobServiceClient, StorageStreamDownloader
import cloudpickle
import s3fs

from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials, AzureSasCredentials
from azure.storage.blob import BlobServiceClient, StorageStreamDownloader, ContainerClient


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
        self.client: BlobServiceClient = client

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        path = pathlib.PurePath(uri.path)
        if path.is_absolute():
            bucket = path.parts[1]
            other = pathlib.PurePath(*path.parts[2:])
        else:
            bucket = path.parts[0]
            other = pathlib.PurePath(*path.parts[1:])

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
            .get_blob_client(str(other))
            .download_blob()
        )
        data = downloader.readall()
        return cloudpickle.loads(data)

    @staticmethod
    def from_connection_string(credentials: AzureCredentials) -> 'AzureWhiteboardStorage':
        return AzureWhiteboardStorage(BlobServiceClient.from_connection_string(credentials.connection_string))

    @staticmethod
    def from_sas(credentials: AzureSasCredentials) -> 'AzureWhiteboardStorage':
        return AzureWhiteboardStorage(BlobServiceClient(credentials.endpoint))


class AmazonWhiteboardStorage(WhiteboardStorage):
    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self.fs_ = s3fs.S3FileSystem(
            key=credentials.access_token,
            secret=credentials.secret_token,
            client_kwargs={"endpoint_url": credentials.endpoint},
        )

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        with self.fs_.open(uri.path) as file:
            return cloudpickle.load(file)
