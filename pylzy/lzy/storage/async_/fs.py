from pathlib import Path
from typing import BinaryIO, Optional, Callable, Any
import os
import shutil
from urllib.parse import urlparse

from lzy.storage.api import AsyncStorageClient


class FsStorageClient(AsyncStorageClient):
    async def size_in_bytes(self, uri: str) -> int:
        path = self.__path_from_uri(uri)
        return os.path.getsize(path)

    async def read(self, uri: str, dest: BinaryIO, progress: Optional[Callable[[int], Any]] = None) -> None:
        path = self.__path_from_uri(uri)
        with open(path, "rb") as file:
            while True:
                read = file.read(8096)
                if not read:
                    break
                if progress:
                    progress(len(read))
                dest.write(read)

    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int], Any]] = None):
        path = self.__path_from_uri(uri)
        parent = Path(path).parent.absolute()
        if not os.path.exists(parent):
            os.makedirs(parent)
        with open(path, "wb") as file:
            while True:
                read = data.read(8096)
                if not read:
                    break
                if progress:
                    progress(len(read))
                file.write(read)

    async def blob_exists(self, uri: str) -> bool:
        path = self.__path_from_uri(uri)
        return os.path.exists(path)

    async def copy(self, from_uri: str, to_uri: str) -> None:
        from_path = self.__path_from_uri(from_uri)
        to_path = self.__path_from_uri(to_uri)
        shutil.copy(from_path, to_path)

    async def sign_storage_uri(self, uri: str) -> str:
        raise NotImplementedError()

    @staticmethod
    def __path_from_uri(uri: str) -> str:
        parsed = urlparse(uri)
        path = os.path.abspath(os.path.join(parsed.netloc, parsed.path))
        return path
