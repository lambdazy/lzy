from functools import singledispatch

from lzy.storage._async.amazon import AmazonClient
from lzy.storage._async.azure import AzureClientAsync
from lzy.storage._async.storage_client import AsyncStorageClient
from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)


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
