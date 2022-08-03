from pathlib import Path
from typing import IO, Any, BinaryIO, cast
from urllib.parse import urlparse

from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from lzy.api.v2.storage.url import Scheme, bucket_from_url, url_from_bucket
from lzy.api.v2.utils import unwrap
from lzy.storage.credentials import AzureCredentials, AzureSasCredentials


class AzureClient:
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

    async def read_to_file(self, url: str, path: Path):
        with path.open("wb") as file:
            blob_client = self._blob_client_from_url(url)
            stream = await blob_client.download_blob()
            async for chunk in stream.chunks():
                file.write(chunk)

    async def read(self, url: str) -> bytes:
        blob_client = self._blob_client_from_url(url)
        stream = await blob_client.download_blob()
        return cast(
            bytes,
            await stream.readall(),
        )

    async def write(self, container: str, blob: str, data: BinaryIO):
        blob_client = self._blob_client(container, blob)
        await blob_client.upload_blob(data)
        return url_from_bucket(self.scheme, container, blob)

    async def blob_exists(self, container: str, blob: str) -> bool:
        blob_client = self._blob_client(container, blob)
        return unwrap(await blob_client.exists())

    @staticmethod
    def from_cred(creds: AzureCredentials) -> "AzureClient":
        return AzureClient(
            BlobServiceClient.from_connection_string(creds.connection_string)
        )

    @staticmethod
    def from_sas_cred(creds: AzureSasCredentials) -> "AzureClient":
        return AzureClient(
            BlobServiceClient.from_connection_string(
                conn_str=creds.endpoint,
                credential=creds.signature,
            )
        )
