from functools import singledispatch
from typing import Union, overload

from ai.lzy.v1.common import storage_pb2
from lzy.storage.api import (
    S3Credentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)

grpc_STORAGE_CREDS = Union[
    storage_pb2.S3Credentials,
    storage_pb2.AzureBlobStorageCredentials,
]


@singledispatch
def _to(credentials: StorageCredentials) -> grpc_STORAGE_CREDS:
    raise NotImplementedError()


@_to.register
def _(credentials: S3Credentials) -> grpc_STORAGE_CREDS:
    return storage_pb2.S3Credentials(
        endpoint=credentials.endpoint,
        accessToken=credentials.access_key_id,
        secretToken=credentials.secret_access_key,
    )


@_to.register  # type: ignore[no-redef]
def _(credentials: AzureCredentials) -> grpc_STORAGE_CREDS:
    return storage_pb2.AzureBlobStorageCredentials(
        connectionString=credentials.connection_string,
    )


@overload
def to(obj: S3Credentials) -> storage_pb2.S3Credentials:
    ...


@overload
def to(obj: AzureCredentials) -> storage_pb2.AzureBlobStorageCredentials:
    ...


@overload
def to(obj: AzureSasCredentials) -> storage_pb2.AzureBlobStorageCredentials:
    ...


def to(obj: StorageCredentials) -> grpc_STORAGE_CREDS:
    return _to(obj)


@singledispatch
def _from(creds: grpc_STORAGE_CREDS) -> StorageCredentials:
    raise NotImplementedError()


@_from.register
def _(creds: storage_pb2.S3Credentials) -> StorageCredentials:
    return S3Credentials(
        access_key_id=creds.accessToken,
        endpoint=creds.endpoint,
        secret_access_key=creds.secretToken,
    )


@_from.register  # type: ignore[no-redef]
def _(creds: storage_pb2.AzureBlobStorageCredentials) -> StorageCredentials:
    return AzureCredentials(
        connection_string=creds.connectionString,
    )


@overload
def from_(obj: storage_pb2.S3Credentials) -> S3Credentials:
    ...


@overload
def from_(obj: storage_pb2.AzureBlobStorageCredentials) -> AzureCredentials:
    ...


def from_(obj: grpc_STORAGE_CREDS) -> StorageCredentials:
    return _from(obj)
