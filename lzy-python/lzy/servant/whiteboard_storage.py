import logging
import pathlib
from abc import ABC, abstractmethod
from typing import TypeVar, Any, Union
from urllib import parse

import cloudpickle
import s3fs

from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials
from azure.storage.blob import BlobServiceClient, StorageStreamDownloader

T = TypeVar("T")


logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class WhiteboardStorage(ABC):

    @abstractmethod
    def read(self, url: str) -> Any:
        pass

    @staticmethod
    def create(credentials: StorageCredentials) -> 'WhiteboardStorage':
        if isinstance(credentials, AmazonCredentials):
            return AmazonWhiteboardStorage(credentials)
        return AzureWhiteboardStorage(credentials)


class AzureWhiteboardStorage(WhiteboardStorage):
    def __init__(self, credentials: AzureCredentials):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.client: BlobServiceClient = BlobServiceClient.from_connection_string(credentials.connection_string)

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

        downloader: StorageStreamDownloader = self.client.get_container_client(bucket).get_blob_client(str(other)).download_blob()
        data = downloader.readall()
        return cloudpickle.loads(data)


class AmazonWhiteboardStorage(WhiteboardStorage):
    def __init__(self, credentials: AmazonCredentials):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.fs = s3fs.S3FileSystem(key=credentials.access_token, secret=credentials.secret_token,
                                    client_kwargs={'endpoint_url': credentials.endpoint})

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        with self.fs.open(uri.path) as f:
            field = cloudpickle.load(f)
            return field
