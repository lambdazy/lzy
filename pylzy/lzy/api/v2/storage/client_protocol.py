from typing import BinaryIO, Protocol


class StorageClient(Protocol):
    async def read(self, url: str, dest: BinaryIO) -> None:
        pass

    async def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass
