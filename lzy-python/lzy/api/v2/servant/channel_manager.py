import abc
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, Union, Set, Optional

from lzy.api.v2.servant.model.channel import Channel, SnapshotChannelSpec, DirectChannelSpec
from lzy.api.v2.servant.model.file_slots import create_slot
from lzy.api.v2.servant.model.slot import Direction, Slot
from lzy.api.v2.servant.servant_client import ServantClient


class ChannelManager(abc.ABC):
    def __init__(self):
        self._snapshot_to_entry_id: Dict[str, Set[str]] = defaultdict(set)
        self._entry_id_to_channel: Dict[str, Channel] = {}

    def channel(self, snapshot_id: str, entry_id: str,
                channel_type: Union[SnapshotChannelSpec, DirectChannelSpec]) -> Channel:
        if entry_id in self._entry_id_to_channel:
            return self._entry_id_to_channel[entry_id]
        channel = Channel(entry_id, channel_type)
        self._create_channel(channel)
        self._entry_id_to_channel[entry_id] = channel
        self._snapshot_to_entry_id[snapshot_id].add(entry_id)
        return channel

    def destroy(self, entry_id: str) -> bool:
        if entry_id not in self._entry_id_to_channel:
            return False
        self._destroy_channel(self._entry_id_to_channel[entry_id])
        self._entry_id_to_channel.pop(entry_id)
        return True

    def destroy_all(self, snapshot_id: Optional[str] = None):
        if snapshot_id is not None:
            for entry in self._snapshot_to_entry_id[snapshot_id]:
                if entry in self._entry_id_to_channel:
                    self.destroy(entry)
            self._snapshot_to_entry_id.pop(snapshot_id)
        else:
            for entry in list(self._entry_id_to_channel):
                self.destroy(entry)
            self._snapshot_to_entry_id = {}

    def in_slot(self, snapshot_id: str, entry_id: str) -> Path:
        return self._resolve(snapshot_id, entry_id, Direction.INPUT)

    def out_slot(self, snapshot_id: str, entry_id) -> Path:
        return self._resolve(snapshot_id, entry_id, Direction.OUTPUT)

    def _resolve(self, snapshot_id: str, entry_id: str, direction: Direction) -> Path:
        slot = create_slot(os.path.sep.join(("tasks", "snapshot", snapshot_id, entry_id)), direction)
        # self._touch(slot, self.channel(snapshot_id, entry_id))
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
    def __init__(self, servant: ServantClient):
        super().__init__()
        self._servant = servant

    def _destroy_channel(self, channel: Channel):
        self._servant.destroy_channel(channel)

    def _touch(self, slot: Slot, channel: Channel):
        self._servant.touch(slot, channel)

    def _resolve_slot_path(self, slot: Slot) -> Path:
        return self._servant.get_slot_path(slot)

    def _create_channel(self, channel: Channel):
        self._servant.create_channel(channel)
