from abc import ABC, abstractmethod
from typing import Optional

from lzy.api.v2.servant.model.channel import Channel
from lzy.api.v2.servant.model.slot import Slot


class ChannelManager(ABC):
    @abstractmethod
    def direct_channel(self, name: str) -> Channel:
        pass

    @abstractmethod
    def snapshot_channel(self, snapshot_id: str, entry_id: str) -> Channel:
        pass

    @abstractmethod
    def destroy(self, channel_name: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    def touch(self, slot: Slot, channel: Channel):
        pass
