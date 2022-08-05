from functools import singledispatch

from ai.lzy.v1 import server_pb2
from lzy.storage.credentials import (
    AmazonCredentials,
    AzureCredentials,
    AzureSasCredentials,
    StorageCredentials,
)


@singledispatch
def to(credentials: AmazonCredentials) -> server_pb2.AmazonCredentials:
    return server_pb2.AmazonCredentials(
        endpoint=credentials.endpoint,
        accessToken=credentials.access_token,
        secretToken=credentials.secret_token,
    )


@to.register  # type: ignore[no-redef]
def to(credentials: AzureCredentials) -> server_pb2.AzureCredentials:
    return server_pb2.AzureCredentials(
        connectionString=credentials.connection_string,
    )


@to.register  # type: ignore[no-redef]
def to(credentials: AzureSasCredentials) -> server_pb2.AzureSASCredentials:
    return server_pb2.AzureSASCredentials(
        endpoint=credentials.endpoint,
        signature=credits.endpoint,
    )


@singledispatch
def from_(creds: server_pb2.AmazonCredentials) -> AmazonCredentials:
    return AmazonCredentials(
        access_token=creds.accessToken,
        endpoint=creds.endpoint,
        secret_token=creds.secretToken,
    )


@from_.register  # type: ignore[no-redef]
def from_(creds: server_pb2.AzureCredentials) -> AzureCredentials:
    return AzureCredentials(
        connection_string=creds.connectionString,
    )


@from_.register  # type: ignore[no-redef]
def from_(creds: server_pb2.AzureSASCredentials) -> AzureSasCredentials:
    return AzureSasCredentials(
        endpoint=creds.endpoint,
        signature=creds.signature,
    )
