from functools import singledispatch
from typing import Union, cast, overload

from ai.lzy.v1.workflow import workflow_pb2
from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)

grpc_STORAGE_CREDS = Union[
    workflow_pb2.AmazonCredentials,
    workflow_pb2.AzureCredentials,
]


@singledispatch
def _to(credentials: StorageCredentials) -> grpc_STORAGE_CREDS:
    raise NotImplementedError()


@_to.register
def _(credentials: AmazonCredentials) -> grpc_STORAGE_CREDS:
    return workflow_pb2.AmazonCredentials(
        endpoint=credentials.endpoint,
        accessToken=credentials.access_token,
        secretToken=credentials.secret_token,
    )


@_to.register  # type: ignore[no-redef]
def _(credentials: AzureCredentials) -> grpc_STORAGE_CREDS:
    return workflow_pb2.AzureCredentials(
        connectionString=credentials.connection_string,
    )


@overload
def to(obj: AmazonCredentials) -> workflow_pb2.AmazonCredentials:
    ...


@overload
def to(obj: AzureCredentials) -> workflow_pb2.AzureCredentials:
    ...


@overload
def to(obj: AzureSasCredentials) -> workflow_pb2.AzureCredentials:
    ...


def to(obj: StorageCredentials) -> grpc_STORAGE_CREDS:
    return _to(obj)


@singledispatch
def _from(creds: grpc_STORAGE_CREDS) -> StorageCredentials:
    raise NotImplementedError()


@_from.register
def _(creds: workflow_pb2.AmazonCredentials) -> StorageCredentials:
    return AmazonCredentials(
        access_token=creds.accessToken,
        endpoint=creds.endpoint,
        secret_token=creds.secretToken,
    )


@_from.register  # type: ignore[no-redef]
def _(creds: workflow_pb2.AzureCredentials) -> StorageCredentials:
    return AzureCredentials(
        connection_string=creds.connectionString,
    )


@overload
def from_(obj: workflow_pb2.AmazonCredentials) -> AmazonCredentials:
    ...


@overload
def from_(obj: workflow_pb2.AzureCredentials) -> AzureCredentials:
    ...


def from_(obj: grpc_STORAGE_CREDS) -> StorageCredentials:
    return _from(obj)
