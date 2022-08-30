from dataclasses import dataclass
from typing import Dict, Optional

from lzy.storage.credentials import StorageCredentials


@dataclass
class Credentials:
    storage_credentials: StorageCredentials
    bucket: str


class StorageRegistry:
    def __init__(self):
        self.__credentials_map: Dict[str, Credentials] = {}
        self.__default: Optional[Credentials] = None
        self.__default_name: Optional[str] = None

    def register_credentials(
        self, name: str, creds: Credentials, default: bool = False
    ):
        self.__credentials_map[name] = creds
        if default:
            self.__default = creds
            self.__default_name = name

    def unregister_credentials(self, name: str):
        self.__credentials_map.pop(name)
        if self.__default_name == name:
            self.__default = None
            self.__default_name = None

    def get_credentials(self, name: str) -> Optional[Credentials]:
        return self.__credentials_map[name]

    def get_default_credentials(self) -> Optional[Credentials]:
        return self.__default
