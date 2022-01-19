import logging
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Mapping, Optional

from lzy.api.whiteboard.credentials import StorageCredentials
from lzy.model.channel import Channel, Bindings
from lzy.model.slot import Slot
from lzy.model.zygote import Zygote


class ExecutionResult:
    def __init__(self, stdout: str, stderr: str, rc: int):
        self._rc = rc
        self._stderr = stderr
        self._stdout = stdout

    def stdout(self) -> str:
        return self._stdout

    def stderr(self) -> str:
        return self._stderr

    def rc(self) -> int:
        return self._rc


class Execution(ABC):
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def bindings(self) -> Bindings:
        pass

    @abstractmethod
    def wait_for(self) -> ExecutionResult:
        pass


class ServantClient(ABC):
    class CredentialsTypes(Enum):
        S3 = "s3"

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
    def run(self, execution_id: str, zygote: Zygote, bindings: Bindings,
            entry_id_mapping: Optional[Mapping[Slot, str]]) -> Execution:
        pass

    @abstractmethod
    def get_credentials(self, typ: CredentialsTypes) -> StorageCredentials:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name}"
