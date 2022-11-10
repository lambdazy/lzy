import datetime
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from grpc.aio import Channel

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from lzy.utils.grpc import build_channel, add_headers_interceptor


class WhiteboardServiceClient(ABC):
    @abstractmethod
    async def get(self, wb_id: str) -> Whiteboard:
        pass

    @abstractmethod
    async def list(
        self,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> Iterable[Whiteboard]:
        pass


class GrpcWhiteboardServiceClient(WhiteboardServiceClient):
    @staticmethod
    async def create(address: str, token: str) -> "GrpcWhiteboardServiceClient":
        channel = build_channel(
            address, interceptors=add_headers_interceptor({"authorization": f"Bearer {token}"})
        )
        await channel.channel_ready()
        stub = LzyWhiteboardServiceStub(channel)
        return GrpcWhiteboardServiceClient(stub, channel)

    def __init__(self, stub: LzyWhiteboardServiceStub, channel: Channel):
        self.__stub = stub
        self.__channel = channel

    async def get(self, wb_id: str) -> Whiteboard:
        resp: GetResponse = await self.__stub.Get(GetRequest(
            whiteboardId=wb_id
        ))
        return resp.whiteboard

    async def list(
        self,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> Iterable[Whiteboard]:
        if not_before is not None:
            from_ = Timestamp()
            from_.FromDatetime(not_before)
        else:
            from_ = None

        if not_after is not None:
            to = Timestamp()
            to.FromDatetime(not_after)
        else:
            to = None

        resp: ListResponse = await self.__stub.List(
            ListRequest(
                name=name if name else "",
                tags=tags,
                createdTimeBounds=TimeBounds(
                    from_=from_,
                    to=to
                )
            )
        )
        return resp.whiteboards
