import datetime
import os
from typing import Optional, Sequence, cast, AsyncIterable

# noinspection PyPackageRequirements
from google.protobuf.timestamp_pb2 import Timestamp
# noinspection PyPackageRequirements
from serialzy.api import SerializerRegistry

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest, \
    RegisterWhiteboardRequest, UpdateWhiteboardRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from lzy.storage.api import StorageRegistry
from lzy.utils.grpc import build_channel, add_headers_interceptor, build_token
from lzy.whiteboards.api import WhiteboardIndexClient, WhiteboardManager
from lzy.whiteboards.wrapper import WhiteboardWrapper

WB_USER_ENV = "LZY_USER"
WB_KEY_PATH_ENV = "LZY_KEY_PATH"
WB_ENDPOINT_ENV = "LZY_WHITEBOARD_ENDPOINT"


class RemoteWhiteboardIndexClient(WhiteboardIndexClient):
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

    async def get(self, id_: str) -> Optional[Whiteboard]:
        self.__start()
        resp: GetResponse = await self.__stub.Get(GetRequest(
            whiteboardId=id_
        ))
        return resp.whiteboard

    async def query(
        self,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> AsyncIterable[Whiteboard]:
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
        # TODO (tomato): make fair pagination
        for wb in resp.whiteboards:
            yield wb

    async def register(self, wb: Whiteboard) -> None:
        self.__start()
        await self.__stub.RegisterWhiteboard(RegisterWhiteboardRequest(whiteboard=wb))

    async def update(self, wb: Whiteboard):
        self.__start()
        await self.__stub.UpdateWhiteboard(UpdateWhiteboardRequest(whiteboard=wb))


class WhiteboardIndexedManager(WhiteboardManager):
    def __init__(self,
                 index_client: WhiteboardIndexClient,
                 storage_registry: StorageRegistry,
                 serializer_registry: SerializerRegistry):
        self.__serializer_registry = serializer_registry
        self.__index_client = index_client
        self.__storage_registry = storage_registry

    async def write_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        await self.__index_client.register(wb)
        # TODO (tomato): write meta to storage

    async def update_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        await self.__index_client.update(wb)
        # TODO (tomato): update meta in storage

    async def get(self,
                  *,
                  id_: Optional[str],
                  storage_uri: Optional[str] = None,
                  storage_name: Optional[str] = None) -> Optional[WhiteboardWrapper]:
        # TODO (tomato): implement reading whiteboard from storage
        if storage_uri is not None or storage_name is not None:
            raise NotImplementedError("Fetching whiteboard by storage uri is not supported yet")

        if id_ is None:
            raise ValueError("ID is not set")

        whiteboard = await self.__index_client.get(id_)
        if whiteboard is None:
            return None
        return WhiteboardWrapper(self.__storage_registry, self.__serializer_registry, whiteboard)

    async def query(self,
                    *,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None,
                    storage_uri: Optional[str] = None,
                    storage_name: Optional[str] = None) -> AsyncIterable[WhiteboardWrapper]:
        if storage_uri is not None or storage_name is not None:
            raise NotImplementedError("Fetching whiteboard by storage uri is not supported yet")

        async for whiteboard in self.__index_client.query(name, tags, not_before, not_after):
            yield WhiteboardWrapper(self.__storage_registry, self.__serializer_registry, whiteboard)
