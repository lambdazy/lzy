from pathlib import Path
from typing import TypeVar, Type, Any

from lzy.api.result import Result
from lzy.model.channel import ChannelManager, Channel
from lzy.model.slot import Slot
from lzy.servant.servant_client import ServantClient


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
        pass

    def read(self, entry_id: str, obj_type: Type[T]) -> Result[Any]:
        pass

    def write(self, entry_id: str, obj: Any):
        pass
