from functools import singledispatch
from typing import Union, overload

from ai.lzy.v1.common import s3_pb2
from lzy.storage.api import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)

grpc_STORAGE_CREDS = Union[
    s3_pb2.AmazonS3Endpoint,
    s3_pb2.AzureS3Endpoint,
]


@singledispatch
def _to(credentials: StorageCredentials) -> grpc_STORAGE_CREDS:
    raise NotImplementedError()


@_to.register
def _(credentials: AmazonCredentials) -> grpc_STORAGE_CREDS:
    return s3_pb2.AmazonS3Endpoint(
        endpoint=credentials.endpoint,
        accessToken=credentials.access_token,
        secretToken=credentials.secret_token,
    )


@_to.register  # type: ignore[no-redef]
def _(credentials: AzureCredentials) -> grpc_STORAGE_CREDS:
    return s3_pb2.AzureS3Endpoint(
        connectionString=credentials.connection_string,
    )


@overload
def to(obj: AmazonCredentials) -> s3_pb2.AmazonS3Endpoint:
    ...


@overload
def to(obj: AzureCredentials) -> s3_pb2.AzureS3Endpoint:
    ...


@overload
def to(obj: AzureSasCredentials) -> s3_pb2.AzureS3Endpoint:
    ...


def to(obj: StorageCredentials) -> grpc_STORAGE_CREDS:
    return _to(obj)


@singledispatch
def _from(creds: grpc_STORAGE_CREDS) -> StorageCredentials:
    raise NotImplementedError()


@_from.register
def _(creds: s3_pb2.AmazonS3Endpoint) -> StorageCredentials:
    return AmazonCredentials(
        access_token=creds.accessToken,
        endpoint=creds.endpoint,
        secret_token=creds.secretToken,
    )


@_from.register  # type: ignore[no-redef]
def _(creds: s3_pb2.AzureS3Endpoint) -> StorageCredentials:
    return AzureCredentials(
        connection_string=creds.connectionString,
    )


@overload
def from_(obj: s3_pb2.AmazonS3Endpoint) -> AmazonCredentials:
    ...


@overload
def from_(obj: s3_pb2.AzureS3Endpoint) -> AzureCredentials:
    ...


def from_(obj: grpc_STORAGE_CREDS) -> StorageCredentials:
    return _from(obj)
