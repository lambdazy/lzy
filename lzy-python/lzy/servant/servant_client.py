from abc import ABC, abstractmethod
import logging
import uuid
from enum import Enum
from pathlib import Path
from typing import Mapping, Optional, Union

from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials
from lzy.model.channel import Channel, Bindings, Binding
from lzy.model.file_slots import create_slot
from lzy.model.slot import Slot, Direction
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

    def run(self, zygote: Zygote, slots_to_entry_id: Optional[Mapping[Slot, str]]) -> Execution:
        execution_id = str(uuid.uuid4())
        self._log.info(f"Running zygote {zygote.name}, execution id {execution_id}")

        bindings = []
        for slot in zygote.slots:
            slot_full_name = "/task/" + execution_id + slot.name
            local_slot = create_slot(slot_full_name, Direction.opposite(slot.direction))
            channel = Channel(':'.join([execution_id, slot.name]))
            self.create_channel(channel)
            self.touch(local_slot, channel)
            bindings.append(Binding(local_slot, slot, channel))

        return self._execute_run(execution_id, zygote, Bindings(bindings), slots_to_entry_id)

    @abstractmethod
    def _execute_run(self, execution_id: str, zygote: Zygote, bindings: Bindings, entry_id_mapping: Optional[Mapping[Slot, str]]) -> Execution:
        pass

    @abstractmethod
    def get_credentials(self, typ: CredentialsTypes) -> StorageCredentials:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name}"
