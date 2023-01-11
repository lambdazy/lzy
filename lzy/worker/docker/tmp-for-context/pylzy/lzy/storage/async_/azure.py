import datetime
from typing import Any, AsyncIterator, BinaryIO

from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient, ContainerClient

# TODO[ottergottaott]: drop this dependency
from lzy.api.v1.utils.types import unwrap
from lzy.storage.api import AsyncStorageClient, AzureCredentials, AzureSasCredentials
from lzy.storage.url import Scheme, bucket_from_uri, uri_from_bucket


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

    def _blob_client_from_uri(self, uri: str) -> Any:
        container, blob = bucket_from_uri(self.scheme, uri)
        return self._blob_client(
            container,
            str(blob),
        )

    async def read(self, uri: str, dest: BinaryIO):
        async for chunk in self.blob_iter(uri):
            dest.write(chunk)

    async def write(self, uri: str, data: BinaryIO):
        container, blob = bucket_from_uri(self.scheme, uri)
        blob_client = self._blob_client(container, blob)
        await blob_client.upload_blob(data)
        return uri_from_bucket(self.scheme, container, blob)

    async def blob_exists(self, uri: str) -> bool:
        container, blob = bucket_from_uri(self.scheme, uri)
        blob_client = self._blob_client(container, blob)
        return unwrap(await blob_client.exists())

    async def blob_iter(self, uri: str) -> AsyncIterator[bytes]:
        blob_client = self._blob_client_from_uri(uri)
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
        return uri_from_bucket(self.scheme, container, blob)

    async def sign_storage_uri(self, uri: str) -> str:
        container, blob = bucket_from_uri(self.scheme, uri)
        sas = generate_blob_sas(
            account_name=self.client.credential.account_name,
            container_name=container,
            blob_name=blob,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1),
            account_key=self.client.credential.account_key,
            start=datetime.datetime.utcnow(),
        )

        return f"https://{self.client.credential.account_name}.blob.core.windows.net/{container}/{blob}?{sas}"
