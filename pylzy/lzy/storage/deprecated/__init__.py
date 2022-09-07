from lzy.storage.api import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)
from lzy.storage.deprecated.amazon import AmazonClient
from lzy.storage.deprecated.azure import AzureClient
from lzy.storage.deprecated.storage_client import StorageClient


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    if isinstance(credentials, AmazonCredentials):
        return AmazonClient(credentials)
    elif isinstance(credentials, AzureCredentials):
        return AzureClient.from_connection_string(credentials)
    elif isinstance(credentials, AzureSasCredentials):
        return AzureClient.from_sas(credentials)
    else:
        raise ValueError("Unknown credentials type: " + type(credentials))
