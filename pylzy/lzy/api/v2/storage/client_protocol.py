from pathlib import Path
from typing import Protocol, BinaryIO


class StorageClient(Protocol):
    async def read(self, url: str) -> bytes:
        pass

    async def read_to_file(self, url: str, filepath: Path) -> None:
        pass

    async def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass
