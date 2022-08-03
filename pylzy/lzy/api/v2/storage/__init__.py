from functools import singledispatch

from lzy.api.v2.storage.amazon import AmazonClient
from lzy.api.v2.storage.azure import AzureClient
from lzy.api.v2.storage.client_protocol import StorageClient
from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)


@singledispatch
def _from(credentials: StorageCredentials) -> StorageClient:
    raise NotImplementedError()


@_from.register
def _(credentials: AmazonCredentials) -> StorageClient:
    return AmazonClient(credentials)


@_from.register
def _(credentials: AzureCredentials) -> StorageClient:
    return AzureClient.from_cred(credentials)


@_from.register
def _(credentials: AzureSasCredentials) -> StorageClient:
    return AzureClient.from_sas_cred(credentials)


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)
