from pathlib import Path
from typing import Protocol, BinaryIO

from functools import singledispatch


class StorageClient(Protocol):
    async def read(self, url: str) -> bytes:
        pass

    async def read_to_file(self, url: str, filename: Path) -> None:
        pass

    async def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass


@singledispatch
def _from(arg) -> StorageClient:
    raise NotImplementedError("_from is not implemented")
