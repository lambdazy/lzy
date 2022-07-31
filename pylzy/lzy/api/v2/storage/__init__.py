from functools import singledispatch
from pathlib import Path
from typing import IO, Any, Tuple
from urllib.parse import urlparse

from typing_extensions import Protocol

# TODO[ottergottaott]: move credentials code to lzy/api/v2
from lzy.storage.credentials import StorageCredentials


class StorageClient(Protocol):
    async def read(self, url: str, dest: IO[Any]) -> Any:
        pass

    async def write(self, container: str, blob: str, data: IO[Any]) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)


@singledispatch
def _from(arg) -> StorageClient:
    raise NotImplementedError("_from is not implemented")


def bucket_from_url(url: str) -> Tuple[str, str]:
    path = Path(urlparse(url).path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, str(Path(*other))
