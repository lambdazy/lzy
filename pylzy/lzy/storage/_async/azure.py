from pathlib import Path
from typing import IO, Any, AsyncIterator, BinaryIO, cast
from urllib.parse import urlparse

from azure.storage.blob.aio import BlobServiceClient, ContainerClient

# TODO[ottergottaott]: drop this dependency
from lzy.api.v2.utils.types import unwrap
from lzy.storage._async import AsyncStorageClient
from lzy.storage.credentials import AzureCredentials, AzureSasCredentials
from lzy.storage.url import Scheme, bucket_from_url, url_from_bucket


class AzureClientAsync(AsyncStorageClient):
    scheme = Scheme.azure

    def __init__(self, client: BlobServiceClient):
        self.client: BlobServiceClient = client

    def _blob_client(
        self,
        container: str,
        blob: str,
    ) -> Any:
        container_client: ContainerClient = self.client.get_container_client(container)
        return container_client.get_blob_client(blob)

    def _blob_client_from_url(self, url: str) -> Any:
        container, blob = bucket_from_url(self.scheme, url)
        return self._blob_client(
            container,
            str(blob),
        )

    async def read(self, url: str, dest: BinaryIO):
        async for chunk in self.blob_iter(url):
            dest.write(chunk)

    async def write(self, container: str, blob: str, data: BinaryIO):
        blob_client = self._blob_client(container, blob)
        await blob_client.upload_blob(data)
        return url_from_bucket(self.scheme, container, blob)

    async def blob_exists(self, container: str, blob: str) -> bool:
        blob_client = self._blob_client(container, blob)
        return unwrap(await blob_client.exists())

    async def blob_iter(self, url: str) -> AsyncIterator[bytes]:
        blob_client = self._blob_client_from_url(url)
        stream = await blob_client.download_blob()
        async for chunk in stream:
            yield chunk

    @staticmethod
    def from_cred(creds: AzureCredentials) -> "AzureClientAsync":
        return AzureClientAsync(
            BlobServiceClient.from_connection_string(creds.connection_string)
        )

    @staticmethod
    def from_sas_cred(creds: AzureSasCredentials) -> "AzureClientAsync":
        return AzureClientAsync(
            BlobServiceClient.from_connection_string(
                conn_str=creds.endpoint,
                credential=creds.signature,
            )
        )

    def generate_uri(self, container: str, blob: str) -> str:
        return url_from_bucket(self.scheme, container, blob)
