from pathlib import Path
from typing import IO, Any
from urllib.parse import urlparse

from azure.storage.blob.aio import (
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)

from lzy.api.v2.storage.bucket import bucket_from_url
from lzy.api.v2.storage.create import _from
from lzy.api.v2.utils import unwrap
from lzy.storage.credentials import (
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)


class AzureClient:
    def __init__(self, client: BlobServiceClient):
        self.client: BlobServiceClient = client

    async def read_to_file(self, url: str, path: Path):
        assert urlparse(url).scheme == "azure"

        bucket, other = bucket_from_url(url)

        # TODO[ottergottaott]: enable
        # downloader: StorageStreamDownloader = (
        #     self.client.get_container_client(bucket)
        #     .get_blob_client(str(other))
        #     .download_blob()
        # )
        # with open(path, "wb") as f:
        #     downloader.readinto(f)

    async def read(self, url: str, dest: IO) -> Any:
        assert urlparse(url).scheme == "azure"

        # TODO[ottergottaott]: enable
        # bucket, other = bucket_from_url(url)
        # downloader: StorageStreamDownloader = (
        #     self.client.get_container_client(bucket)
        #     .get_blob_client(str(other))
        #     .download_blob()
        # )
        return None

    async def write(self, container: str, blob: str, data: IO):
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(data)  # type: ignore
        return self.generate_uri(container, blob)

    async def blob_exists(self, container: str, blob: str) -> bool:
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        return unwrap(await blob_client.exists())

    def generate_uri(self, container: str, blob: str) -> str:
        return f"azure:/{container}/{blob}"


@_from.register
def _(credentials: AzureCredentials) -> AzureClient:
    return AzureClient(
        BlobServiceClient.from_connection_string(credentials.connection_string)
    )


@_from.register
def _(credentials: AzureSasCredentials) -> AzureClient:
    return AzureClient(BlobServiceClient(credentials.endpoint))
