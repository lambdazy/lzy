import abc
import os
import tempfile
from pathlib import Path
from typing import TypeVar, Dict, List

from lzy.api.v1.servant.model.channel import Channel, SnapshotChannelSpec
from lzy.api.v1.servant.model.file_slots import create_slot
from lzy.api.v1.servant.model.slot import Slot, Direction
from lzy.api.v1.servant.servant_client import ServantClient


class ChannelManager(abc.ABC):
    def __init__(self, snapshot_id: str):
        self._entry_id_to_channel: Dict[str, Channel] = {}
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

    def destroy_all(self):
        for entry in list(self._entry_id_to_channel):
            self.destroy(entry)

    def in_slot(self, entry_id: str) -> Path:
        return self._resolve(entry_id, Direction.INPUT)

    def out_slot(self, entry_id) -> Path:
        return self._resolve(entry_id, Direction.OUTPUT)

    def _resolve(self, entry_id: str, direction: Direction) -> Path:
        slot = create_slot(os.path.sep.join(("tasks", "snapshot", self._snapshot_id, entry_id)), direction)
        self._touch(slot, self.channel(entry_id))
        path = self._resolve_slot_path(slot)
        return path

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


class ServantChannelManager(ChannelManager):
    def __init__(self, snapshot_id: str, servant: ServantClient):
        super(ServantChannelManager, self).__init__(snapshot_id)
        self._servant = servant

    def _destroy_channel(self, channel: Channel):
        self._servant.destroy_channel(channel)

    def _touch(self, slot: Slot, channel: Channel):
        self._servant.touch(slot, channel)

    def _resolve_slot_path(self, slot: Slot) -> Path:
        return self._servant.get_slot_path(slot)

    def _create_channel(self, channel: Channel):
        self._servant.create_channel(channel)


T = TypeVar("T")


class LocalChannelManager(ChannelManager):
    def __init__(self, snapshot_id: str):
        super(LocalChannelManager, self).__init__(snapshot_id)
        self._tmp_files: List[str] = []

    def _create_channel(self, channel: Channel):
        pass

    def _destroy_channel(self, channel: Channel):
        pass

    def _touch(self, slot: Slot, channel: Channel):
        pass

    def _resolve_slot_path(self, slot: Slot) -> Path:
        pass

    def channel(self, entry_id: str) -> Channel:
        pass

    def destroy(self, entry_id: str):
        pass

    def destroy_all(self):
        for path in self._tmp_files:
            os.remove(path)

    def _resolve(self, entry_id: str, direction: Direction) -> Path:
        file = tempfile.NamedTemporaryFile()
        self._tmp_files.append(file.name)
        return Path(file.name)
