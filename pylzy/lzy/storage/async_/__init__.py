from functools import singledispatch

from lzy.storage.api import (
    AmazonCredentials,
    AsyncStorageClient,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)


# noinspection PyUnusedLocal
@singledispatch
def _from(credentials: StorageCredentials) -> AsyncStorageClient:
    raise NotImplementedError()


@_from.register
def _(credentials: AmazonCredentials) -> AsyncStorageClient:
    # use local imports to import unnecessary libs as late as possible to avoid version conflicts
    from lzy.storage.async_.amazon import AmazonClient

    return AmazonClient(credentials)


@_from.register
def _(credentials: AzureCredentials) -> AsyncStorageClient:
    # use local imports to import unnecessary libs as late as possible to avoid version conflicts
    from lzy.storage.async_.azure import AzureClientAsync

    return AzureClientAsync.from_cred(credentials)


@_from.register
def _(credentials: AzureSasCredentials) -> AsyncStorageClient:
    # use local imports to import unnecessary libs as late as possible to avoid version conflicts
    from lzy.storage.async_.azure import AzureClientAsync

    return AzureClientAsync.from_sas_cred(credentials)


def from_credentials(credentials: StorageCredentials) -> AsyncStorageClient:
    return _from(credentials)
