import abc
import os.path
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union, Dict, TypeVar, Type, Tuple, Iterable, Any
from lzy.api.result import Result, Just, Nothing
from lzy.api.serializer.serializer import Serializer
from lzy.api.utils import LzyExecutionException
from lzy.model.file_slots import create_slot

from lzy.model.slot import Slot, Direction


@dataclass
class SnapshotChannelSpec:
    snapshot_id: str
    entry_id: str


@dataclass
class DirectChannelSpec:
    pass


@dataclass
class Channel:
    name: str
    spec: Union[SnapshotChannelSpec, DirectChannelSpec]


@dataclass
class Binding:
    slot: Slot
    channel: Channel


Bindings = List[Binding]


T = TypeVar("T")


class ChannelManager(abc.ABC):
    def __init__(self, snapshot_id: str):
        self._entry_id_to_channel: Dict[str, Channel] = {}
        self._entry_id_written: Dict[str, bool] = defaultdict(lambda: False)
        self._snapshot_id = snapshot_id

    def channel(self, entry_id: str) -> Channel:
        if entry_id in self._entry_id_to_channel:
            return self._entry_id_to_channel[entry_id]
        channel = Channel(entry_id, SnapshotChannelSpec(self._snapshot_id, entry_id))
        self._create_channel(channel)
        self._entry_id_to_channel[entry_id] = channel
        return channel

    def destroy(self, entry_id: str):
        if entry_id not in self._entry_id_to_channel:
            return
        self._destroy_channel(self._entry_id_to_channel[entry_id])
        self._entry_id_to_channel.pop(entry_id)
        if entry_id in self._entry_id_written:
            self._entry_id_written.pop(entry_id)

    def destroy_all(self):
        for entry in list(self._entry_id_to_channel):
            self.destroy(entry)

    def __slot(self, entry_id: str, direction: Direction) -> Slot:
        slot = create_slot(os.path.sep.join(("tasks", "snapshot", self._snapshot_id, entry_id)), direction)
        self._touch(slot, self.channel(entry_id))
        return slot

    def __read(self, entry_id: str, typ: Type[T]) -> T:
        slot = self.__slot(entry_id, Direction.INPUT)
        path = self._resolve_slot_path(slot)
        with path.open("rb") as handle:
            # Wait for slot to become open
            while handle.read(1) is None:
                time.sleep(0)  # Thread.yield
                if not path.exists():
                    raise LzyExecutionException("Cannot read from slot")
            handle.seek(0)
            return Serializer.deserialize_from_file(handle, typ)

    def read(self, entry_id: str, obj_type: Type[T]) -> Result[Any]:
        # noinspection PyBroadException
        try:
            return Just(self.__read(entry_id, obj_type))
        except (OSError, ValueError) as _:
            return Nothing()
        except BaseException as _:  # pylint: disable=broad-except
            return Nothing()

    def write(self, entry_id: str, obj: Any):
        if self._entry_id_written[entry_id]:
            raise ValueError("Cannot write to already written channel")
        slot = self.__slot(entry_id, Direction.OUTPUT)
        path = self._resolve_slot_path(slot)
        with path.open("wb") as f:
            Serializer.serialize_to_file(obj, f, type(obj))
            self._entry_id_written[entry_id] = True
            f.flush()
            os.fsync(f.fileno())

    @abc.abstractmethod
    def _create_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def _destroy_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def _touch(self, slot: Slot, channel: Channel):
        pass

    @abc.abstractmethod
    def _resolve_slot_path(self, slot: Slot) -> Path:
        pass
