from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional, Set

from betterproto import which_one_of as which

from lzy.api.v1.servant.bash_servant_client import BashServantClient
from lzy.api.v2.grpc.servant.grpc_calls import Run
from lzy.api.v2.servant.model.file_slots import create_slot
from lzy.proto.bet.priv.v2 import (
    Auth,
    Channel,
    ChannelCommand,
    ChannelCreate,
    ChannelDestroy,
    ChannelState,
    ChannelStatus,
    CreateSlotCommand,
    DataScheme,
    DirectChannelSpec,
    Slot,
    SlotCommand,
    SlotCommandStatus,
    SlotDirection,
    SnapshotChannelSpec,
)
from lzy.proto.priv.v2.lzy_fs_grpc import LzyFsStub
from lzy.proto.priv.v2.lzy_server_grpc import LzyServerStub


class ChannelManager:
    def __init__(self, snapshot_id: str, channel_api: "ChannelApi", fs_api: "FsApi"):
        self._entry_id_to_channel: Dict[str, Channel] = {}
        self._snapshot_id = snapshot_id
        self._channel_api = channel_api
        self._fs_api = fs_api

    async def channel(self, entry_id: str, type_: type) -> Channel:
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

    async def destroy(self, entry_id: str):
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
        self, entry_id: str, direction: SlotDirection, data_scheme: DataScheme
    ) -> Path:
        path = Path("tasks") / "snapshot" / self._snapshot_id / entry_id
        slot = create_slot(
            str(path),
            direction,
            data_scheme,
        )
        self._touch(slot, entry_id)
        path = self._resolve_slot_path(slot)
        return path

    async def _touch(self, slot: Slot, entry_id: str):
        channel_status: ChannelStatus = await self._channel_api.channel_state(entry_id)
        channel = channel_status.channel

        status: SlotCommandStatus = await self._fs_api.configure_slot(slot, entry_id)
        print(status)

    def _resolve_slot_path(self, slot: Slot) -> Path:
        pass


class FsApi(ABC):
    @abstractmethod
    async def configure_slot(
        self,
        slot: Slot,
        channel_id: str,
        is_pipe: bool = False,
    ) -> SlotCommandStatus:
        pass


class GrpcFsApi(FsApi):
    def __init__(self, auth: Auth, fs_stub: LzyFsStub):
        self.auth = auth
        self.fs_stub = fs_stub

    async def configure_slot(
        self,
        slot: Slot,
        channel_id: str,
        is_pipe: bool = False,
    ) -> SlotCommandStatus:
        name, value = which(self.auth, "credenctials")
        tid = value.task_id if name == "task" else f"user-{value.user_id}"

        create = CreateSlotCommand(slot=slot, channel_id=channel_id, is_pipe=is_pipe)
        slot_cmd = SlotCommand(tid=tid, slot=slot, create=create)

        return await self.fs_stub.ConfigureSlot(slot_cmd)


class ChannelApil(ABC):
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
        pid: int,
        name: str,
        pipe: bool,
        channel_id: str,
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
