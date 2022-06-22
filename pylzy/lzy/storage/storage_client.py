import logging
import os.path
import pathlib
from abc import ABC, abstractmethod
from typing import Any, TypeVar, Tuple, IO
from urllib import parse

import boto3
from azure.storage.blob import BlobServiceClient, StorageStreamDownloader, ContainerClient
from botocore.exceptions import ClientError
from pure_protobuf.dataclasses_ import loads, load  # type: ignore

from lzy.storage.credentials import AzureCredentials, AmazonCredentials, AzureSasCredentials, StorageCredentials

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

T = TypeVar("T")  # pylint: disable=invalid-name


class StorageClient(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str, dest: IO):
        pass

    @abstractmethod
    def write(self, container: str, blob: str, data: IO) -> str:
        pass

    @abstractmethod
    def blob_exists(self, container: str, blob: str) -> bool:
        pass

    @abstractmethod
    def generate_uri(self, container: str, blob: str) -> str:
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

    def read(self, url: str, dest: IO) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        bucket, other = bucket_from_url(url)

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
                .get_blob_client(str(other))
                .download_blob()
        )
        data = downloader.readinto(dest)
        return data

    def write(self, container: str, blob: str, data: IO):
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(data)  # type: ignore
        return self.generate_uri(container, blob)

    def blob_exists(self, container: str, blob: str) -> bool:
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        return blob_client.exists()

    def generate_uri(self, container: str, blob: str) -> str:
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

    def read(self, url: str, dest: IO) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        bucket, key = bucket_from_url(url)
        self._client.download_fileobj(bucket, key, dest)

    def write(self, bucket: str, key: str, data: IO) -> str:
        self._client.upload_fileobj(data, bucket, key)
        return self.generate_uri(bucket, key)

    def blob_exists(self, container: str, blob: str) -> bool:
        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            self._client.head_object(Bucket=container, Key=blob)
            return True
        except ClientError:
            return False

    def generate_uri(self, bucket: str, key: str) -> str:
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
