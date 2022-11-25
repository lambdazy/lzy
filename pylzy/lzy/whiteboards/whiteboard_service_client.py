import datetime
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from grpc.aio import Channel

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds, Storage, WhiteboardFieldInfo
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest, \
    CreateWhiteboardRequest, CreateWhiteboardResponse, FinalizeWhiteboardRequest, LinkFieldRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from serialzy.api import Schema
from lzy.utils.grpc import build_channel, add_headers_interceptor
from lzy.whiteboards.whiteboard_declaration import WhiteboardInstanceMeta, WhiteboardField


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

    @abstractmethod
    async def create_whiteboard(
        self,
        namespace: str,
        name: str,
        fields: Sequence[WhiteboardField],
        storage_name: str,
        tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:
        pass

    @abstractmethod
    async def link(self, wb_id: str, field_name: str, url: str, data_scheme: Schema) -> None:
        pass

    @abstractmethod
    async def finalize(
        self,
        whiteboard_id: str
    ):
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

    async def create_whiteboard(
        self,
        namespace: str,
        name: str,
        fields: Sequence[WhiteboardField],
        storage_name: str,
        tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:

        res: CreateWhiteboardResponse = await self.__stub.CreateWhiteboard(
            CreateWhiteboardRequest(
                whiteboardName=name,
                namespace=namespace,
                storage=Storage(
                    name=storage_name,
                    description=""
                ),
                tags=tags,
                fields=[
                    WhiteboardFieldInfo(
                        name=field.name,
                        linkedState=WhiteboardFieldInfo.LinkedField(
                            storageUri=field.default.url,
                            scheme=self.__build_scheme(field.default.data_scheme)
                        ) if field.default else None,
                        noneState=WhiteboardFieldInfo.NoneField() if field.default is None else None
                    ) for field in fields
                ]
            )
        )

        return WhiteboardInstanceMeta(res.whiteboard.id)

    async def link(self, wb_id: str, field_name: str, url: str, data_scheme: Schema) -> None:
        await self.__stub.LinkField(
            LinkFieldRequest(
                whiteboardId=wb_id,
                fieldName=field_name,
                storageUri=url,
                scheme=self.__build_scheme(data_scheme)
            )
        )

    async def finalize(
        self,
        whiteboard_id: str
    ):
        await self.__stub.FinalizeWhiteboard(
            FinalizeWhiteboardRequest(
                whiteboardId=whiteboard_id
            )
        )

    @staticmethod
    def __build_scheme(data_scheme: Schema) -> DataScheme:
        return DataScheme(
            dataFormat=data_scheme.data_format,
            schemeFormat=data_scheme.schema_format,
            schemeContent=data_scheme.schema_content
            if data_scheme.schema_content else "",
            metadata=data_scheme.meta
        )