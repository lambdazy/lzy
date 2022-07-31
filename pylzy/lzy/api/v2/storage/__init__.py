import logging
from typing import IO, Any, TypeVar

from typing_extensions import Protocol

from lzy.api.v2.storage.amazon import AmazonClient
from lzy.api.v2.storage.azure import AzureClient
from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

T = TypeVar("T")  # pylint: disable=invalid-name


class StorageClient(Protocol):
    async def read(self, url: str, dest: IO[Any]) -> Any:
        pass

    async def write(self, container: str, blob: str, data: IO[Any]) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    if isinstance(credentials, AmazonCredentials):
        return AmazonClient(credentials)
    elif isinstance(credentials, AzureCredentials):
        return AzureClient.from_connection_string(credentials)
    elif isinstance(credentials, AzureSasCredentials):
        return AzureClient.from_sas(credentials)
    else:
        raise ValueError(f"Unknown credentials type: {type(credentials)}")
