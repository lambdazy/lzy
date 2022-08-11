import logging
from typing import BinaryIO
from urllib import parse

from azure.storage.blob import (  # type: ignore
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)

# TODO[ottergottaott]: drop this dependency
from lzy.api.v2.utils.types import unwrap
from lzy.storage.credentials import AzureCredentials, AzureSasCredentials
from lzy.storage.url import Scheme, bucket_from_url

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


class AzureClient:
    scheme = Scheme.azure

    def read_to_file(self, url: str, path: str):
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        bucket, other = bucket_from_url(self.scheme, url)

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

    def read(self, url: str, dest: BinaryIO) -> None:
        uri = parse.urlparse(url)
        assert uri.scheme == "azure"
        bucket, other = bucket_from_url(self.scheme, url)

        downloader: StorageStreamDownloader = (
            self.client.get_container_client(bucket)
            .get_blob_client(str(other))
            .download_blob()
        )
        # data = downloader.readinto(dest)
        # return data
        downloader.readinto(dest)

    def write(self, container: str, blob: str, data: BinaryIO):
        container_client: ContainerClient = self.client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(data)  # type: ignore
        return self.generate_uri(container, blob)

    def blob_exists(self, container: str, blob: str) -> bool:
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
