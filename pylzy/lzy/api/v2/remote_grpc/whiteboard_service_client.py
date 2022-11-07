from typing import Iterable

from google.protobuf.timestamp_pb2 import Timestamp
from grpc.aio import Channel

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from lzy.api.v2 import Query
from lzy.api.v2.remote_grpc.utils import build_channel, add_headers_interceptor


class WhiteboardServiceClient:
    @staticmethod
    async def create(address: str, token: str) -> "WhiteboardServiceClient":
        channel = build_channel(
            address, interceptors=add_headers_interceptor({"authorization": f"Bearer {token}"})
        )
        await channel.channel_ready()
        stub = LzyWhiteboardServiceStub(channel)
        return WhiteboardServiceClient(stub, channel)

    def __init__(self, stub: LzyWhiteboardServiceStub, channel: Channel):
        self.__stub = stub
        self.__channel = channel

    async def get(self, wb_id: str) -> Whiteboard:
        resp: GetResponse = await self.__stub.Get(GetRequest(
            whiteboardId=wb_id
        ))
        return resp.whiteboard

    async def list(self, query: Query) -> Iterable[Whiteboard]:
        if query.not_before is not None:
            from_ = Timestamp()
            from_.FromDatetime(query.not_before)
        else:
            from_ = None

        if query.not_after is not None:
            to = Timestamp()
            to.FromDatetime(query.not_after)
        else:
            to = None

        resp: ListResponse = await self.__stub.List(
            ListRequest(
                name=query.name,
                tags=query.tags,
                createdTimeBounds=TimeBounds(
                    from_=from_,
                    to=to
                )
            )
        )
        return resp.whiteboards