import abc
import logging
import uuid
from pathlib import Path

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


class Execution:
    @abc.abstractmethod
    def id(self) -> str:
        pass

    @abc.abstractmethod
    def bindings(self) -> Bindings:
        pass

    @abc.abstractmethod
    def wait_for(self) -> ExecutionResult:
        pass


class ServantClient:
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))

    @abc.abstractmethod
    def mount(self) -> Path:
        pass

    @abc.abstractmethod
    def get_slot_path(self, slot: Slot) -> Path:
        pass

    @abc.abstractmethod
    def create_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def destroy_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def touch(self, slot: Slot, channel: Channel):
        pass

    @abc.abstractmethod
    def publish(self, zygote: Zygote):
        pass

    def run(self, zygote: Zygote) -> Execution:
        execution_id = str(uuid.uuid4())
        self._log.info(f"Running zygote {zygote.name()}, execution id {execution_id}")

        bindings = []
        for slot in zygote.slots():
            slot_full_name = "/task/" + execution_id + slot.name()
            local_slot = create_slot(slot_full_name, Direction.opposite(slot.direction()))
            channel = Channel(':'.join([execution_id, slot.name()]))
            self.create_channel(channel)
            self.touch(local_slot, channel)
            bindings.append(Binding(local_slot, slot, channel))

        return self._execute_run(execution_id, zygote, Bindings(bindings))

    @abc.abstractmethod
    def _execute_run(self, execution_id: str, zygote: Zygote, bindings: Bindings) -> Execution:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name()}"
