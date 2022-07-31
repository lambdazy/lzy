from typing import IO, Any

from typing_extensions import Protocol


class StorageClient(Protocol):
    async def read(self, url: str, dest: IO[Any]) -> Any:
        pass

    async def write(self, container: str, blob: str, data: IO[Any]) -> str:
        pass

    async def blob_exists(self, container: str, blob: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass
