import logging
import os.path
import pathlib
import tempfile
from abc import ABC, abstractmethod
from typing import Any, Tuple, BinaryIO
from urllib import parse
import boto3
import cloudpickle
from urllib.parse import urlunsplit

from azure.storage.blob import BlobServiceClient, StorageStreamDownloader, ContainerClient

from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials, AzureSasCredentials

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


class StorageClient(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str) -> Any:
        pass

    @abstractmethod
    def read_to_file(self, url: str, path: str):
        pass

    @abstractmethod
    def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass


class AzureClient(StorageClient):
    def read_to_file(self, url: str, path: str):
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        bucket, other = bucket_from_url(url)

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
                .get_blob_client(str(other))
                .download_blob()
        )
        with open(path, "wb") as f:
            downloader.readinto(f)

    def __init__(self, client: BlobServiceClient):
        super().__init__()
        self.client: BlobServiceClient = client

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        bucket, other = bucket_from_url(url)

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
                .get_blob_client(str(other))
                .download_blob()
        )
        data = downloader.readall()
        return cloudpickle.loads(data)

    def write(self, container: str, blob: str, data: BinaryIO):
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        if not blob_client.exists():
            blob_client.upload_blob(data)
        return f"azure:/{container}/{blob}"

    @staticmethod
    def from_connection_string(credentials: AzureCredentials) -> 'AzureClient':
        return AzureClient(BlobServiceClient.from_connection_string(credentials.connection_string))

    @staticmethod
    def from_sas(credentials: AzureSasCredentials) -> 'AzureClient':
        return AzureClient(BlobServiceClient(credentials.endpoint))


class AmazonClient(StorageClient):
    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self._client = boto3.client(
            's3',
            aws_access_key_id=credentials.access_token,
            aws_secret_access_key=credentials.secret_token,
            endpoint_url=credentials.endpoint
        )
        self.__logger = logging.getLogger(self.__class__.__name__)

    def read_to_file(self, url: str, path: str):
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        bucket, key = bucket_from_url(url)
        with open(path, "wb") as file:
            self._client.download_fileobj(bucket, key, file)

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        bucket, key = bucket_from_url(url)
        with tempfile.TemporaryFile() as file:
            self._client.download_fileobj(bucket, key, file)
            file.seek(0)
            return cloudpickle.load(file)

    def write(self, bucket: str, key: str, data: BinaryIO) -> str:
        self._client.upload_fileobj(data, bucket, key)
        path = os.path.join(bucket, key)
        return f"s3:/{path}"


def bucket_from_url(url: str) -> Tuple[str, str]:
    uri = parse.urlparse(url)
    path = pathlib.PurePath(uri.path)
    if path.is_absolute():
        bucket = path.parts[1]
        other = pathlib.PurePath(*path.parts[2:])
    else:
        bucket = path.parts[0]
        other = pathlib.PurePath(*path.parts[1:])
    return bucket, str(other)


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    if isinstance(credentials, AmazonCredentials):
        return AmazonClient(credentials)
    elif isinstance(credentials, AzureCredentials):
        return AzureClient.from_connection_string(credentials)
    elif isinstance(credentials, AzureSasCredentials):
        return AzureClient.from_sas(credentials)
    else:
        raise ValueError("Unknown credentials type: " + type(credentials))
