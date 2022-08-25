from dataclasses import dataclass
from typing import Optional, Dict

from lzy.storage.credentials import StorageCredentials


@dataclass
class Credentials:
    name: str
    storage_credentials: StorageCredentials
    bucket: str


class StorageRegistry:
    def __init__(self):
        self.__credentials_map: Dict[str, Credentials] = {}
        self.__default: Optional[Credentials] = None

    def register_credentials(self, creds: Credentials, default: bool = False):
        self.__credentials_map[creds.name] = creds
        if default:
            self.__default = creds

    def get_credentials(self, name: str) -> Optional[Credentials]:
        return self.__credentials_map[name]

    def get_default_credentials(self) -> Optional[Credentials]:
        return self.__default
