from typing import List

from google.protobuf.empty_pb2 import Empty
from grpclib.client import Channel

from ai.lzy.v1.portal_grpc import LzyPortalStub
from ai.lzy.v1.portal_pb2 import (
    OpenSlotsRequest,
    OpenSlotsResponse,
    PortalSlotDesc,
    PortalStatus,
    SaveSnapshotSlotRequest,
    SaveSnapshotSlotResponse,
    StartPortalRequest,
    StartPortalResponse,
)
from ai.lzy.v1.zygote_pb2 import Slot


class Portal:
    def __init__(self, channel: Channel):
        self.stub = LzyPortalStub(channel)

    async def start(
        self,
        stdout_channel_id: str,
        stderr_channel_id: str,
    ) -> StartPortalResponse:
        return await self.stub.Start(
            StartPortalRequest(
                stdoutChannelId=stdout_channel_id,
                stderrChannelId=stderr_channel_id,
            )
        )

    async def stop(self):
        await self.stub.Stop(Empty)

    async def status(self) -> PortalStatus:
        return await self.stub.Status(Empty)

    async def open_slots(self, slots: List[PortalSlotDesc]):
        return await self.stub.OpenSlots(OpenSlotsRequest(slots=slots))

    async def save_snapshot_slot(
        self, save_request: SaveSnapshotSlotRequest
    ) -> SaveSnapshotSlotResponse:
        return await self.stub.SaveSnapshotSlot(save_request)
