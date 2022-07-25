from typing import List

from google.protobuf.empty_pb2 import Empty
from grpclib.client import Channel

from ai.lzy.v1 import (
    OpenSlotsRequest,
    OpenSlotsResponse,
    PortalSlotDesc,
    PortalStatus,
    SaveSnapshotSlotRequest,
    SaveSnapshotSlotResponse,
    StartPortalRequest,
    StartPortalResponse,
    Slot,
)
from ai.lzy.v1.portal_grpc import LzyPortalStub


class Portal:
    def __init__(self, channel: Channel):
        self.stub = LzyPortalStub(channel)

    async def start(
        self,
        stdout_channel_id: str,
        stderr_channel_id: str,
    ):
        start_request = StartPortalRequest(
            stdout_channel_id,
            stderr_channel_id,
        )
        return await self.stub.Start(start_request)

    async def stop(self):
        await self.stub.Stop(Empty)

    async def status(self) -> PortalStatus:
        return await self.stub.Status(Empty)

    async def open_slots(self, slots: List[PortalSlotDesc]):
        return await self.stub.OpenSlots(OpenSlotsRequest(slots))

    async def save_snapshot_slot(
        self, save_request: SaveSnapshotSlotRequest
    ) -> SaveSnapshotSlotResponse:
        return await self.stub.SaveSnapshotSlot(save_request)
