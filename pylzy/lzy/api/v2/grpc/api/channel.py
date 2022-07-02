from abc import ABC, abstractmethod
from typing import Optional

from betterproto import which_one_of as which

from lzy.proto.bet.priv.v2 import DataScheme, ChannelStatus, Slot, SlotCommandStatus, Auth, LzyFsStub, \
    DirectChannelSpec, SnapshotChannelSpec, ChannelCreate, ChannelCommand, ChannelState, ChannelDestroy, \
    CreateSlotCommand, SlotCommand
from lzy.proto.priv.v2.lzy_server_grpc import LzyServerStub


class ChannelApi(ABC):
    @abstractmethod
    async def create_direct_channel(
            self,
            name: str,
            content_type: DataScheme,
    ) -> ChannelStatus:
        pass

    @abstractmethod
    async def create_snapshot_channel(
            self,
            name: str,
            content_type: DataScheme,
            snapshot_id: str,
            entry_id: str,
    ) -> ChannelStatus:
        pass

    @abstractmethod
    async def channel_state(
            self,
            name: str,
    ) -> ChannelStatus:
        pass

    @abstractmethod
    async def destroy_channel(self, channel_name: str):
        pass

    @abstractmethod
    async def create_slot(
            self,
            slot: Slot,
            channel_id: str,
            tid: Optional[str] = None,
            pipe: bool = False,
    ) -> SlotCommandStatus:
        pass

    @abstractmethod
    async def resolve_slot(self, slot: Slot) -> str:
        pass

    @abstractmethod
    def close(self):
        pass


class ChannelGrpcApi(ChannelApi):
    def __init__(
            self,
            auth: Auth,
            server: LzyServerStub,
            servant_fs: LzyFsStub,
    ):
        self.server = server
        self.servant_fs = servant_fs
        self.auth = auth
        self.pid = -1  # TODO[ottergottaott]

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
            # is it useless?
            auth=self.auth,
            channel_name=name,
            create=create,
        )
        return await self.server.Channel(channel_cmd)

    async def channel_state(
            self,
            name: str,
    ) -> ChannelStatus:
        channel_cmd = ChannelCommand(
            auth=self.auth,
            channel_name=name,
            state=ChannelState(),
        )
        return await self.server.Channel(channel_cmd)

    async def destroy_channel(self, channel_name: str) -> ChannelStatus:
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
            channel_id: str,
            tid: Optional[str] = None,
            pipe: bool = False,
    ) -> SlotCommandStatus:
        create = CreateSlotCommand(
            slot=slot,
            channel_id=channel_id,
            is_pipe=pipe,
        )

        # LOG.info("Create {}slot `{}` ({}) for channel `{}` with taskId {}.",
        #         pipe ? "pipe " : "", slotName, name, channelId, pid);
        #

        name, value = which(self.auth, "credenctials")
        if tid is None:
            tid = value.task_id if name == "task" else f"user-{value.user_id}"

        slot_cmd = SlotCommand(
            tid=tid,
            slot=slot.name,
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
