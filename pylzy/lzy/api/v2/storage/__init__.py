from lzy.storage.credentials import StorageCredentials

from lzy.api.v2.storage.client_protocol import StorageClient, _from
from lzy.api.v2.storage.amazon import _ as __am  # just call module to register func
from lzy.api.v2.storage.azure import _ as __az  # just call module to register func


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)
