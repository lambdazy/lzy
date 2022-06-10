from typing import Dict, Optional

from lzy.api.v2.grpc.servant.api.channel_manager import ChannelManager
from lzy.api.v2.servant.model.channel import Channel, SnapshotChannelSpec, DirectChannelSpec
from lzy.api.v2.servant.model.slot import Slot


class ChannelManagerImpl(ChannelManager):
    def __init__(self):
        self._name_to_channel: Dict[str, Channel] = {}

    def direct_channel(self, name: str) -> Channel:
        if name in self._name_to_channel:
            return self._name_to_channel[name]
        channel = Channel(name, DirectChannelSpec())
        self._create_channel(channel)
        self._name_to_channel[name] = channel
        return channel

    def snapshot_channel(self, snapshot_id: str, entry_id: str) -> Channel:
        if entry_id in self._name_to_channel:
            return self._name_to_channel[entry_id]
        channel = Channel(entry_id, SnapshotChannelSpec(snapshot_id, entry_id))
        self._create_channel(channel)
        self._name_to_channel[entry_id] = channel
        return channel

    def destroy(self, channel_name: Optional[str] = None) -> bool:
        if channel_name is None:
            for entry in list(self._name_to_channel):
                self.destroy(entry)
            self._name_to_channel = {}
            return True
        else:
            if channel_name not in self._name_to_channel:
                return False
            self._destroy_channel(self._name_to_channel[channel_name])
            self._name_to_channel.pop(channel_name)
            return True

    def touch(self, slot: Slot, channel: Channel):
        # TODO: implement
        pass

    def _create_channel(self, channel: Channel):
        # TODO: implement
        pass

    def _destroy_channel(self, channel: Channel):
        # TODO: implement
        pass
