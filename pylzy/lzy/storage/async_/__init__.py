from functools import singledispatch

from lzy.storage.api import (
    AmazonCredentials,
    AsyncStorageClient,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)
from lzy.storage.async_.amazon import AmazonClient
from lzy.storage.async_.azure import AzureClientAsync


@singledispatch
def _from(credentials: StorageCredentials) -> AsyncStorageClient:
    raise NotImplementedError()


@_from.register
def _(credentials: AmazonCredentials) -> AsyncStorageClient:
    return AmazonClient(credentials)


@_from.register
def _(credentials: AzureCredentials) -> AsyncStorageClient:
    return AzureClientAsync.from_cred(credentials)


@_from.register
def _(credentials: AzureSasCredentials) -> AsyncStorageClient:
    return AzureClientAsync.from_sas_cred(credentials)


def from_credentials(credentials: StorageCredentials) -> AsyncStorageClient:
    return _from(credentials)
