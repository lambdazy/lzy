from lzy.storage import from_credentials
from lzy.storage.credentials import StorageCredentials
from lzy.storage.storage_client import StorageClient


class WhiteboardStorage:
    @staticmethod
    def create(credentials: StorageCredentials) -> StorageClient:
        return from_credentials(credentials)
