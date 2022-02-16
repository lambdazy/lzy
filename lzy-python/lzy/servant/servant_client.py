from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping, Optional

from lzy.api.whiteboard.credentials import (
    # AzureCredentials,
    # AmazonCredentials,
    StorageCredentials,
)
from lzy.model.channel import Channel, Bindings
from lzy.model.slot import Slot
from lzy.model.zygote import Zygote


@dataclass
class ExecutionResult:
    stdout: str
    stderr: str
    returncode: int


class Execution(ABC):
    # pylint: disable=invalid-name
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def bindings(self) -> Bindings:
        pass

    @abstractmethod
    def wait_for(self) -> ExecutionResult:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class CredentialsTypes(Enum):
    S3 = "s3"


class ServantClient(ABC):
    def __init__(self):
        super().__init__()
        self._log = logging.getLogger(str(self.__class__))

    @abstractmethod
    def mount(self) -> Path:
        pass

    @abstractmethod
    def get_slot_path(self, slot: Slot) -> Path:
        pass

    @abstractmethod
    def create_channel(self, channel: Channel):
        pass

    @abstractmethod
    def destroy_channel(self, channel: Channel):
        pass

    @abstractmethod
    def touch(self, slot: Slot, channel: Channel):
        pass

    @abstractmethod
    def publish(self, zygote: Zygote):
        pass

    @abstractmethod
    def run(
            self,
            execution_id: str,
            zygote: Zygote,
            bindings: Bindings,
            entry_id_mapping: Optional[Mapping[Slot, str]]) -> Execution:
        pass

    @abstractmethod
    def get_credentials(self, typ: CredentialsTypes, bucket: str) -> StorageCredentials:
        pass

    @abstractmethod
    def get_bucket(self) -> str:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name}"
