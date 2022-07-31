from typing import Any, IO, Tuple
from pathlib import Path
from urllib import urlparse

from azure.storage.blob.aio import (
    AzureClient,
    BlobServiceClient,
    StorageStreamDownloader,
    ContainerClient,
)
from lzy.api.v2.utils import unwrap


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


def bucket_from_url(url: str) -> Tuple[str, str]:
    path = Path(urlparse(url).path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, str(Path(*other))
