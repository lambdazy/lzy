from urllib.parse import urlsplit

from pathlib import Path
from functools import singledispatch
from typing import Any, Protocol, Tuple, BinaryIO

from lzy.storage.credentials import StorageCredentials


class StorageClient(Protocol):
    async def read(self, url: str) -> bytes:
        pass

    async def read_to_file(self, url: str, filename: Path) -> None:
        pass

    async def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass


def from_credentials(credentials: StorageCredentials) -> StorageClient:
    return _from(credentials)


@singledispatch
def _from(arg) -> StorageClient:
    raise NotImplementedError("_from is not implemented")


def url_from_bucket(
    scheme: str,
    bucket: str,
    key: str,
) -> str:
    path = Path(f"{scheme}:") / bucket / key
    return str(path)


def bucket_from_url(scheme: str, url: Path) -> Tuple[str, Path]:
    _parsed_scheme, _, path, _, _ = urlsplit(url)
    assert _parsed_scheme == scheme

    path = Path(path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, Path(*other)
