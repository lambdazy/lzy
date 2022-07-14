from abc import ABC, abstractmethod
from typing import Dict, Optional

from lzy.api.v2.grpc.servant.grpc_calls import Run
from lzy.api.v2.servant.model.channel import (
    Channel,
    DirectChannelSpec,
    SnapshotChannelSpec,
)
from lzy.api.v2.servant.model.slot import Slot
from lzy.proto.bet.priv.v2 import (
    Auth,
    Channel,
    ChannelCommand,
    ChannelCreate,
    ChannelDestroy,
    ChannelStatus,
    CreateSlotCommand,
    DataScheme,
    DirectChannelSpec,
    SlotCommand,
    SlotCommandStatus,
    SnapshotChannelSpec,
)
from lzy.proto.priv.v2.lzy_fs_grpc import LzyFsStub
from lzy.proto.priv.v2.lzy_server_grpc import LzyServerStub


class ChannelManager:
    def __init__(
        self,
        auth: Auth,
        server: LzyServerStub,
        servant_fs: LzyFsStub,
    ):
        self.server = server
        self.servant_fs = servant_fs
        self.auth = Auth("", "")  # TODO
        self.pid = -1

    async def create_direct_channel(
        self,
        name: str,
        content_type: DataScheme,
    ) -> ChannelStatus:
        return await self.create_channel(
            name,
            content_type,
            direct=DirectChannelSpec(),
        )

    async def create_snapshot_channel(
        self,
        name: str,
        content_type: DataScheme,
        snapshot_id: str,
        entry_id: str,
    ) -> ChannelStatus:
        snapshot = SnapshotChannelSpec(snapshot_id, entry_id)
        return await self.create_channel(
            name,
            content_type,
            snapshot=snapshot,
        )

    # kwargs trick:
    async def create_channel(
        self,
        name: str,
        content_type: DataScheme,
        **type_,
    ) -> ChannelStatus:
        # LOG.info("Create channel `{}` for slot `{}`.", channelName, slot.name());
        create = ChannelCreate(
            content_type=content_type,
            **type_,
        )
        channel_cmd = ChannelCommand(
            auth=self.auth,
            channel_name=name,
            create=create,
        )
        return await self.server.Channel(channel_cmd)

    async def destroy_channel(self, channel_name: str):
        destroy = ChannelDestroy()
        channel_cmd = ChannelCommand(
            auth=self.auth,
            channel_name=channel_name,
            destroy=destroy,
        )
        # TODO[aleksZubakov]: try/catch here?
        return await self.server.Channel(channel_cmd)

    async def create_slot(
        self,
        slot: Slot,
        pid: int,
        name: str,
        pipe: bool,
        channel_id: str,
    ) -> SlotCommandStatus:
        create = CreateSlotCommand(
            slot=slot,
            channel_id=channel_id,
            is_pipe=pipe,
        )
        # LOG.info("Create {}slot `{}` ({}) for channel `{}` with taskId {}.",
        #         pipe ? "pipe " : "", slotName, name, channelId, pid);
        slot_cmd = SlotCommand(
            tid=str(pid),
            slot=name,
            create=create,
        )
        return await self.servant_fs.ConfigureSlot(slot_cmd)

    async def resolve_slot(self, slot: Slot) -> str:
        # LOG.info("Resolving slot " + slot.name());
        #
        # final String binding;
        # if (slot.media() == Slot.Media.ARG) {
        #     binding = String.join(" ", command.getArgList().subList(1, command.getArgList().size()));
        # } else if (bindings.containsKey(slot.name())) {
        #     binding = "channel:" + bindings.get(slot.name());
        # } else {
        #     binding = "channel:" + resolveChannel(slot);
        # }
        # LOG.info("Slot " + slot.name() + " resolved to " + binding);
        #
        binding = ""
        return binding

    def close(self):
        self._chan.close()
