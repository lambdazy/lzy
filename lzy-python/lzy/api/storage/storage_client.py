import pathlib
from abc import ABC, abstractmethod
from typing import Any
from urllib import parse
from urllib.parse import urlunsplit

from azure.storage.blob import BlobServiceClient, StorageStreamDownloader, ContainerClient
import cloudpickle
import s3fs
import logging
from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, AzureSasCredentials, StorageCredentials


class StorageClient(ABC):
    def __init__(self):
        # pylint: disable=unused-private-member
        self.__logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, url: str) -> Any:
        pass

    @abstractmethod
    def write(self, container: str, blob: str, data):
        pass


class AzureClient(StorageClient):
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

    def write(self, container: str, blob: str, data):
        container_client: ContainerClient = self.client.get_container_client(container)
        container_client.get_blob_client(blob).upload_blob(data)
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
        self.fs_ = s3fs.S3FileSystem(
            key=credentials.access_token,
            secret=credentials.secret_token,
            client_kwargs={"endpoint_url": credentials.endpoint},
        )
        self.__logger = logging.getLogger(self.__class__.__name__)

    def read(self, url: str) -> Any:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        with self.fs_.open(uri.path) as file:
            return cloudpickle.load(file)

    def write(self, bucket: str, key: str, data) -> str:
        path = f"/{bucket}/{key}"
        url = urlunsplit(("s3", "", path, "", ""))
        uri = parse.urlparse(url)
        self.fs_.mkdirs(f"{bucket}", exist_ok=True)
        self.fs_.touch(f"{bucket}/{key}")
        with self.fs_.open(uri.path, "wb") as file:
            file.write(data)
        return url


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    if isinstance(credentials, AmazonCredentials):
        return AmazonClient(credentials)
    elif isinstance(credentials, AzureCredentials):
        return AzureClient.from_connection_string(credentials)
    elif isinstance(credentials, AzureSasCredentials):
        return AzureClient.from_sas(credentials)
    else:
        raise ValueError("Unknown credentials type: " + type(credentials))
