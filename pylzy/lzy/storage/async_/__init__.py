from functools import singledispatch

from lzy.storage.api import (
    S3Credentials,
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
def _(credentials: S3Credentials) -> AsyncStorageClient:
    # use local imports to import unnecessary libs as late as possible to avoid version conflicts
    from lzy.storage.async_.s3 import S3Client

    return S3Client(credentials)


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
