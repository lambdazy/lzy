from abc import ABC, abstractmethod
from typing import BinaryIO


class AsyncStorageClient(ABC):
    @abstractmethod
    async def read(self, url: str, dest: BinaryIO) -> None:
        pass

    @abstractmethod
    async def write(self, container: str, blob: str, data: BinaryIO) -> str:
        """
        @return: url in what data was written
        """
        pass

    @abstractmethod
    async def blob_exists(self, container: str, blob: str) -> bool:
        pass
