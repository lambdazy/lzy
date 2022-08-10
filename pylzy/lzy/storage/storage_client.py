from typing import BinaryIO, Protocol


class StorageClient(Protocol):
    def read(self, url: str, dest: BinaryIO):
        pass

    def write(self, container: str, blob: str, data: BinaryIO) -> str:
        pass

    def blob_exists(self, container: str, blob: str) -> bool:
        pass

    def generate_uri(self, container: str, blob: str) -> str:
        pass
