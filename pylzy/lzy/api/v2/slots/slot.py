import abc
from enum import Enum
from typing import Optional


class SlotState(Enum):
    UNBOUND = 0
    PREPARING = 1
    OPEN = 2
    SUSPENDED = 3
    CLOSED = 4
    DESTROYED = 5


class SlotDirection(Enum):
    INPUT = 1
    OUTPUT = 2


class Slot(abc.ABC):
    def __init__(self, name: str, channel_name: str, direction: SlotDirection):
        self._state = SlotState.UNBOUND
        self._name = name
        self._channel_name = channel_name
        self._direction = direction

    @property
    def name(self) -> str:
        return self._name

    @property
    def channel_name(self) -> str:
        return self._channel_name

    @property
    def direction(self) -> SlotDirection:
        return self._direction

    @property
    def state(self) -> SlotState:
        return self._state

    @property
    def uri(self) -> str:
        return ""  # TODO(artolord) implement this

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()

    @abc.abstractmethod
    async def close(self):
        """
        Method to close input slot from terminal
        """
        pass

    @abc.abstractmethod
    async def suspend(self):
        """
        Call from kharon
        """
        pass

    @abc.abstractmethod
    async def destroy(self):
        """
        Call from kharon
        """
        pass


class OutputSlot(abc.ABC, Slot):
    def __init__(self, name: str, channel_name: str):
        super(OutputSlot, self).__init__(name, channel_name, SlotDirection.OUTPUT)

    @abc.abstractmethod
    async def write(self, data: bytes):
        pass


class InputSlot(abc.ABC, Slot):
    def __init__(self, name: str, channel_name: str):
        super(InputSlot, self).__init__(name, channel_name, SlotDirection.INPUT)

    @abc.abstractmethod
    async def read(self, num: int) -> Optional[bytes]:
        pass


class SlotManager(abc.ABC):

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    async def open_input(self, channel_name: str, slot_name: str) -> InputSlot:
        pass

    @abc.abstractmethod
    async def open_output(self, channel_name: str, slot_name: str) -> InputSlot:
        pass
