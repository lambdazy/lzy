from functools import singledispatch

from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)
from lzy.api.v2.storage.client_protocol import StorageClient, _from
from lzy.api.v2.storage.amazon import AmazonClient
from lzy.api.v2.storage.azure import AzureClient


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)


@singledispatch
@_from.register
def _(credentials: AmazonCredentials) -> AmazonClient:
    return AmazonClient(credentials)


@_from.register
def _(credentials: AzureCredentials) -> AzureClient:
    return AzureClient.from_cred(credentials)


@_from.register
def _(credentials: AzureSasCredentials) -> AzureClient:
    return AzureClient.from_sas_cred(credentials)
