import datetime
import json
import os
import tempfile
from json import JSONDecodeError
from typing import Optional, Sequence, cast, AsyncIterable, BinaryIO

from google.protobuf.json_format import MessageToJson, Parse, ParseDict
# noinspection PyPackageRequirements
from google.protobuf.timestamp_pb2 import Timestamp
# noinspection PyPackageRequirements
from serialzy.api import SerializerRegistry

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, TimeBounds
from ai.lzy.v1.whiteboard.whiteboard_service_pb2 import GetRequest, GetResponse, ListResponse, ListRequest, \
    RegisterWhiteboardRequest, UpdateWhiteboardRequest
from ai.lzy.v1.whiteboard.whiteboard_service_pb2_grpc import LzyWhiteboardServiceStub
from lzy.api.v1 import WorkflowServiceClient
from lzy.storage.api import StorageRegistry, AsyncStorageClient
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

    async def __start(self) -> None:
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
        await self.__channel.channel_ready()

        self.__stub = LzyWhiteboardServiceStub(self.__channel)

    async def get(self, id_: str) -> Optional[Whiteboard]:
        await self.__start()
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
        await self.__start()
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
        await self.__start()
        await self.__stub.RegisterWhiteboard(RegisterWhiteboardRequest(whiteboard=wb))

    async def update(self, wb: Whiteboard):
        await self.__start()
        await self.__stub.UpdateWhiteboard(UpdateWhiteboardRequest(whiteboard=wb))


class WhiteboardIndexedManager(WhiteboardManager):
    def __init__(self,
                 workflow_client: Optional[WorkflowServiceClient],
                 index_client: WhiteboardIndexClient,
                 storage_registry: StorageRegistry,
                 serializer_registry: SerializerRegistry):
        self.__workflow_client = workflow_client
        self.__index_client = index_client
        self.__storage_registry = storage_registry
        self.__serializer_registry = serializer_registry

    async def write_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        storage_client = await self.__resolve_storage_client(storage_name)
        with tempfile.NamedTemporaryFile() as f:
            f.write(MessageToJson(wb).encode('UTF-8'))
            f.seek(0)
            await storage_client.write(f"{uri}/.whiteboard", cast(BinaryIO, f))

        await self.__index_client.register(wb)

    async def update_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        storage_client = await self.__resolve_storage_client(storage_name)
        wb_meta_uri = f"{uri}/.whiteboard"

        full_wb = await self.__get_meta_from_storage(storage_client, wb_meta_uri)
        if full_wb is None:
            raise ValueError(f"Whiteboard with id {wb.id} not found, nothing to update")

        updated_wb = Whiteboard(id=wb.id,
                                name=wb.name if wb.name else full_wb.name,
                                tags=wb.tags if wb.tags else full_wb.tags,
                                fields=wb.fields if wb.fields else full_wb.fields,
                                storage=wb.storage if wb.HasField("storage") else full_wb.storage,
                                namespace=wb.namespace if wb.namespace else full_wb.namespace,
                                status=wb.status if wb.status else full_wb.status,
                                createdAt=wb.createdAt if wb.HasField("createdAt") else full_wb.createdAt)

        with tempfile.NamedTemporaryFile() as f:
            f.write(MessageToJson(updated_wb).encode('UTF-8'))
            f.seek(0)
            await storage_client.write(wb_meta_uri, cast(BinaryIO, f))

        # Does it really necessary?
        if updated_wb.status == Whiteboard.Status.FINALIZED:
            wb_finalized_marker_uri = f"{uri}/.finalized"
            with tempfile.NamedTemporaryFile() as f:
                await storage_client.write(wb_finalized_marker_uri, cast(BinaryIO, f))

        await self.__index_client.update(wb)

    async def get(self,
                  *,
                  id_: Optional[str],
                  storage_uri: Optional[str] = None,
                  storage_name: Optional[str] = None) -> Optional[WhiteboardWrapper]:

        if storage_uri is None:
            if id_ is None:
                raise ValueError("Neither id nor uri are set")

            index_client_wb = await self.__index_client.get(id_)
            if index_client_wb is None:
                return None

            storage_uri = index_client_wb.storage.uri
        else:
            index_client_wb = None

        storage_client = await self.__resolve_storage_client(storage_name)
        wb_meta_uri = f"{storage_uri}/.whiteboard"

        wb = await self.__get_meta_from_storage(storage_client, wb_meta_uri)
        if wb is None:
            return None

        if id_ is not None and id_ != wb.id:
            raise ValueError(
                f"Id mismatch, requested whiteboard with id {id_}, found in storage whiteboard with id {wb.id}"
            )

        if index_client_wb is None:
            index_client_wb = await self.__index_client.get(wb.id)

        if wb != index_client_wb:
            await self.__index_client.update(wb)

        return WhiteboardWrapper(self.__storage_registry, self.__serializer_registry, wb)

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

        await self.__update_default_storage()

        async for whiteboard in self.__index_client.query(name, tags, not_before, not_after):
            yield WhiteboardWrapper(self.__storage_registry, self.__serializer_registry, whiteboard)

    async def __resolve_storage_client(self, storage_name: Optional[str]) -> AsyncStorageClient:
        if storage_name is not None:
            storage_client = self.__storage_registry.client(storage_name)
            if storage_client is None:
                raise RuntimeError(f"No storage client with name {storage_name}")
        else:
            storage_client = self.__storage_registry.default_client()
            if storage_client is None:
                await self.__update_default_storage()
                storage_client = self.__storage_registry.default_client()
                if storage_client is None:
                    raise RuntimeError("No default storage client")

        return storage_client

    async def __update_default_storage(self):
        if self.__workflow_client is not None:
            storage_creds = await self.__workflow_client.get_default_storage()
            storage_name = self.__storage_registry.provided_storage_name()
            self.__storage_registry.register_storage(storage_name, storage_creds, default=True)

    async def __get_meta_from_storage(self,
                                      storage_client: AsyncStorageClient,
                                      wb_meta_uri: str) -> Optional[Whiteboard]:

        exists = await storage_client.blob_exists(wb_meta_uri)
        if not exists:
            return None

        with tempfile.TemporaryFile() as f:
            await storage_client.read(wb_meta_uri, cast(BinaryIO, f))
            f.seek(0)
            try:
                wb = ParseDict(json.load(f), Whiteboard())
            except JSONDecodeError as e:
                raise RuntimeError("Whiteboard corrupted, failed to load")

        return wb
