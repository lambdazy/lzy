import datetime
import time
from typing import Any, AsyncIterator, BinaryIO, cast, Optional, Callable

# noinspection PyPackageRequirements
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
# noinspection PyPackageRequirements
from azure.storage.blob.aio import BlobServiceClient, ContainerClient

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

    async def size_in_bytes(self, uri: str) -> int:
        container, blob = bucket_from_uri(self.scheme, uri)
        blob_client = self._blob_client(container, blob)
        props = await blob_client.get_blob_properties()
        return cast(int, props.size)

    async def read(self, uri: str, dest: BinaryIO, progress: Optional[Callable[[int], Any]] = None):
        async for chunk in self.blob_iter(uri):
            dest.write(chunk)

    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int], Any]] = None):
        container, blob = bucket_from_uri(self.scheme, uri)
        blob_client = self._blob_client(container, blob)
        await blob_client.upload_blob(data, prpgress_hook=lambda x, t: progress(x) if progress else None)
        return uri_from_bucket(self.scheme, container, blob)

    async def blob_exists(self, uri: str) -> bool:
        container, blob = bucket_from_uri(self.scheme, uri)
        blob_client = self._blob_client(container, blob)
        return cast(bool, await blob_client.exists())

    async def blob_iter(self, uri: str, progress: Optional[Callable[[int], None]] = None) -> AsyncIterator[bytes]:
        blob_client = self._blob_client_from_uri(uri)
        stream = await blob_client.download_blob(progress_hook=lambda x, t: progress(x) if progress else None)
        async for chunk in stream:
            yield chunk

    async def copy(self, from_uri: str, to_uri: str) -> None:
        url = self.sign_storage_uri(from_uri)
        blob_client = self._blob_client_from_uri(to_uri)
        await blob_client.start_copy_from_url(url)
        while True:
            props = await blob_client.get_blob_properties()
            status = props.copy.status
            if status == "success":
                break
            elif status == "pending":
                time.sleep(1)
            else:
                await blob_client.abort_copy(props.copy.id)
                raise ValueError(f"Failed to copy object with status {status}")

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
