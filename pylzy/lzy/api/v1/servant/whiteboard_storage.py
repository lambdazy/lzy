from lzy.storage.api import StorageCredentials
from lzy.storage.deprecated import StorageClient, from_credentials


class WhiteboardStorage:
    @staticmethod
    def create(credentials: StorageCredentials) -> StorageClient:
        return from_credentials(credentials)
