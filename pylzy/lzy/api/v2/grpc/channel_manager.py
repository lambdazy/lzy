from pathlib import Path
from typing import Dict

from lzy.api.v2.servant.model.slot import file_slot_t
from lzy.proto.bet.priv.v2 import (
    Channel,
    ChannelStatus,
    DataScheme,
    Slot,
    SlotDirection,
)

from lzy.api.v2.grpc.api.channel import ChannelApi

class ChannelManager:
    def __init__(self, snapshot_id: str, channel_api: ChannelApi):
        self._entry_id_to_channel: Dict[str, Channel] = {}
        self._snapshot_id = snapshot_id
        self._channel_api = channel_api

    async def channel(self, entry_id: str, type_: type) -> ChannelStatus:
        if entry_id in self._entry_id_to_channel:
            return self._entry_id_to_channel[entry_id]

        result: ChannelStatus = await self._channel_api.create_snapshot_channel(
            entry_id,
            type_,
            self._snapshot_id,
            entry_id,
        )

        self._entry_id_to_channel[entry_id] = result.channel
        # TODO[ottergottaott]: what todo with connected slots here?
        return result.channel

    async def destroy(self, entry_id: str) -> ChannelStatus:
        if entry_id not in self._entry_id_to_channel:
            return

        channel_status: ChannelStatus = await self._channel_api.destroy_channel(
            entry_id
        )
        raise NotImplementedError(f"Should do something with {channel_status}")
        self._entry_id_to_channel.pop(entry_id, None)

    async def destroy_all(self):
        for entry in self._entry_id_to_channel.keys():
            await self.destroy(entry)

    def in_slot(self, entry_id: str, data_scheme: DataScheme) -> Path:
        return self._resolve(entry_id, SlotDirection.INPUT, data_scheme)

    def out_slot(self, entry_id: str, data_scheme: DataScheme) -> Path:
        return self._resolve(entry_id, SlotDirection.OUTPUT, data_scheme)

    def _resolve(
            self, entry_id: str, direction: SlotDirection, type_: type
    ) -> Path:
        path = Path("tasks") / "snapshot" / self._snapshot_id / entry_id
        slot = file_slot_t(path, direction, type_)
        await self._channel_api.create_slot(slot, entry_id)
        return self._resolve_slot_path(slot)

    def _resolve_slot_path(self, slot: Slot) -> Path:
        pass