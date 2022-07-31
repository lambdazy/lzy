from functools import singledispatch
from typing import TYPE_CHECKING

# TODO[ottergottaott]: move credentials code to lzy/api/v2
from lzy.storage.credentials import StorageCredentials

if TYPE_CHECKING:
    from lzy.api.v2.storage import StorageClient


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)


@singledispatch
def _from(arg) -> StorageClient:
    raise NotImplementedError("_from is not implemented")
