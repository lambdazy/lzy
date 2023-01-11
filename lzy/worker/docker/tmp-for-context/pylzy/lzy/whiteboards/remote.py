import datetime
import os
from typing import Iterable, Optional, Sequence, cast

# noinspection PyPackageRequirements
from google.protobuf.timestamp_pb2 import Timestamp
# noinspection PyPackageRequirements
from serialzy.api import Schema

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds, Storage, WhiteboardFieldInfo
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest, \
    CreateWhiteboardRequest, CreateWhiteboardResponse, FinalizeWhiteboardRequest, LinkFieldRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from lzy.utils.grpc import build_channel, add_headers_interceptor, build_token
from lzy.whiteboards.api import WhiteboardClient, WhiteboardField, WhiteboardInstanceMeta

WB_USER_ENV = "LZY_USER"
WB_KEY_PATH_ENV = "LZY_KEY_PATH"
WB_ENDPOINT_ENV = "LZY_WHITEBOARD_ENDPOINT"


class RemoteWhiteboardClient(WhiteboardClient):
    def __init__(self):
        self.__channel = None
        self.__stub = None
        self.__is_started = False

    def __start(self) -> None:
        if self.__is_started:
            return
        self.__is_started = True

        user = os.getenv(WB_USER_ENV)
        key_path = os.getenv(WB_KEY_PATH_ENV)
        endpoint: str = os.getenv(WB_ENDPOINT_ENV, "api.lzy.ai:8899")

        if user is None:
            raise ValueError(f"User must be specified by env variable {WB_USER_ENV} or `user` argument")
        if key_path is None:
            raise ValueError(f"Key path must be specified by env variable {WB_KEY_PATH_ENV} or `key_path` argument")

        token = build_token(cast(str, user), cast(str, key_path))
        self.__channel = build_channel(
            endpoint, interceptors=add_headers_interceptor({"authorization": f"Bearer {token}"})
        )
        self.__stub = LzyWhiteboardServiceStub(self.__channel)

    async def get(self, wb_id: str) -> Whiteboard:
        self.__start()
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
        self.__start()
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
        self.__start()
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

        return WhiteboardInstanceMeta(res.whiteboard.id, name, tags)

    async def link(self, wb_id: str, field_name: str, url: str, data_scheme: Schema) -> None:
        self.__start()
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
        self.__start()
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
