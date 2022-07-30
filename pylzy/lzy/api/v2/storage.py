import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, Any, Tuple, TypeVar
from urllib.parse import urlparse

from typing_extensions import Protocol

from lzy.api.v2.utils import unwrap

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

T = TypeVar("T")  # pylint: disable=invalid-name


class StorageClient(Protocol):
    @abstractmethod
    async def read(self, url: str, dest: IO[Any]) -> Any:
        pass

    @abstractmethod
    async def write(self, container: str, blob: str, data: IO[Any]) -> str:
        pass

    @abstractmethod
    async def blob_exists(self, container: str, blob: str) -> bool:
        pass

    @abstractmethod
    async def generate_uri(self, container: str, blob: str) -> str:
        pass


class AzureClient:
    def __init__(self, client: BlobServiceClient):
        self.client: BlobServiceClient = client

    async def read_to_file(self, url: str, path: str):
        assert urlparse(url).scheme == "azure"

        bucket, other = bucket_from_url(url)

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
            .get_blob_client(str(other))
            .download_blob()
        )
        with open(path, "wb") as f:
            downloader.readinto(f)

    async def read(self, url: str, dest: IO) -> Any:
        assert urlparse(url).scheme == "azure"

        bucket, other = bucket_from_url(url)
        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
            .get_blob_client(str(other))
            .download_blob()
        )
        return downloader.readinto(dest)

    async def write(self, container: str, blob: str, data: IO):
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(data)  # type: ignore
        return self.generate_uri(container, blob)

    async def blob_exists(self, container: str, blob: str) -> bool:
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        return unwrap(blob_client.exists())

    def generate_uri(self, container: str, blob: str) -> str:
        return f"azure:/{container}/{blob}"

    @staticmethod
    def from_connection_string(credentials: AzureCredentials) -> "AzureClient":
        return AzureClient(
            BlobServiceClient.from_connection_string(credentials.connection_string)
        )

    @staticmethod
    def from_sas(credentials: AzureSasCredentials) -> "AzureClient":
        return AzureClient(BlobServiceClient(credentials.endpoint))


class AmazonClient(StorageClient):
    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self._client = boto3.client(
            "s3",
            aws_access_key_id=credentials.access_token,
            aws_secret_access_key=credentials.secret_token,
            endpoint_url=credentials.endpoint,
        )
        self.__logger = logging.getLogger(self.__class__.__name__)

    async def read(self, url: str, dest: IO) -> Any:
        assert urlparse(url).scheme == "s3"

        bucket, key = bucket_from_url(url)
        self._client.download_fileobj(bucket, key, dest)

    async def write(self, bucket: str, key: str, data: IO) -> str:
        self._client.upload_fileobj(data, bucket, key)
        return self.generate_uri(bucket, key)

    async def blob_exists(self, container: str, blob: str) -> bool:
        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            self._client.head_object(Bucket=container, Key=blob)
            return True
        except ClientError:
            return False

    def generate_uri(self, bucket: str, key: str) -> str:
        path = Path(bucket) / key
        return f"s3:/{path}"


def bucket_from_url(url: str) -> Tuple[str, str]:
    path = Path(urlparse(url).path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, str(Path(*other))


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    if isinstance(credentials, AmazonCredentials):
        return AmazonClient(credentials)
    elif isinstance(credentials, AzureCredentials):
        return AzureClient.from_connection_string(credentials)
    elif isinstance(credentials, AzureSasCredentials):
        return AzureClient.from_sas(credentials)
    else:
        raise ValueError("Unknown credentials type: " + type(credentials))
