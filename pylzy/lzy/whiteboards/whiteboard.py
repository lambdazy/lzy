import asyncio
import datetime
import os
import tempfile
from typing import List, Any, TypeVar, Optional, Union, Iterable, Dict, Sequence

from lzy.proxy.result import Nothing

from lzy.utils.event_loop import LzyEventLoop

from lzy.serialization.api import SerializerRegistry, Schema
from lzy.storage.api import StorageRegistry, AsyncStorageClient

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, WhiteboardField
from lzy.utils.grpc import build_token
from lzy.whiteboards.whiteboard_service_client import GrpcWhiteboardServiceClient, WhiteboardServiceClient

LZY_WHITEBOARD_ADDRESS_ENV = "LZY_WHITEBOARD_ADDRESS"


T = TypeVar("T")  # pylint: disable=invalid-name


class WhiteboardRepository:

    @staticmethod
    def with_grpc_client(
        storage: StorageRegistry,
        serializers: SerializerRegistry,
        username: Optional[str] = None,
        address: Optional[str] = None,
        key_path: Optional[str] = None
    ) -> "WhiteboardRepository":

        addr = (
            address
            if address is not None
            else os.getenv(LZY_WHITEBOARD_ADDRESS_ENV, "api.lzy.ai:8899")
        )

        token = build_token(username, key_path)

        client = LzyEventLoop.run_async(GrpcWhiteboardServiceClient.create(addr, token))
        return WhiteboardRepository(
            storage, serializers, client
        )

    def __init__(
        self,
        storage: StorageRegistry,
        serializers: SerializerRegistry,
        client: WhiteboardServiceClient
    ):
        self.__storage = storage
        self.__serializers = serializers
        self.__client = client

    def get(self, wb_id: str) -> Any:
        wb: Whiteboard = LzyEventLoop.run_async(self.__client.get(wb_id))
        return LzyEventLoop.run_async(self.__build_whiteboard(wb))

    def list(
        self, *,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> List[Any]:
        wbs: Iterable[Whiteboard] = LzyEventLoop.run_async(self.__client.list(
            name, tags, not_before, not_after
        ))
        wbs_to_build = [self.__build_whiteboard(wb) for wb in wbs]
        return list(LzyEventLoop.run_async(asyncio.gather(*wbs_to_build)))

    async def __build_whiteboard(self, wb: Whiteboard) -> Any:

        if wb.status != Whiteboard.Status.FINALIZED:
            raise RuntimeError(f"Status of whiteboard with name {wb.name} is {wb.status}, but must be COMPLETED")

        return _ReadOnlyWhiteboard(self.__storage, self.__serializers, wb)


class _ReadOnlyWhiteboard:
    def __init__(
            self,
            storage: StorageRegistry,
            serializers: SerializerRegistry,
            wb: Whiteboard
    ):
        cli = storage.client(wb.storage.name)

        if cli is None:
            raise RuntimeError(f"Storage {wb.storage.name} not found in your registry")

        self.__storage = cli

        self.__serializers = serializers
        self.__wb = wb
        self.__fields: Dict[str, Union[WhiteboardField, Any]] = {
            field.info.name: field for field in wb.fields
        }

    def __getattr__(self, item: str) -> Any:
        var = self.__fields.get(item, Nothing())

        if isinstance(var, Nothing):
            raise AttributeError(f"Whiteboard field {item} not found")

        if isinstance(var, WhiteboardField):
            var = LzyEventLoop.run_async(self.__read_data(var, self.__storage))
            self.__fields[item] = var
            return var

        return item

    @property
    def whiteboard_id(self) -> str:
        return self.__wb.id

    @property
    def whiteboard_name(self) -> str:
        return self.__wb.name

    @property
    def whiteboard_tags(self) -> Iterable[str]:
        return self.__wb.tags

    async def __read_data(self, field: WhiteboardField, client: AsyncStorageClient) -> Any:
        if field.status != WhiteboardField.Status.FINALIZED:
            raise RuntimeError(f"Whiteboard field {field.info.name} is not finalized")

        data_scheme = field.info.linkedState.scheme

        serializer = self.__serializers.find_serializer_by_data_format(data_scheme.dataFormat)

        if serializer is None:
            raise RuntimeError(f"Serializer not found for data format {data_scheme.dataFormat}")

        schema = Schema(
            data_format=data_scheme.dataFormat,
            schema_format=data_scheme.schemeFormat,
            schema_content=data_scheme.schemeContent,
            meta=dict(**data_scheme.metadata)
        )
        typ = serializer.resolve(schema)

        storage_uri = field.info.linkedState.storageUri

        exists = await client.blob_exists(storage_uri)
        if not exists:
            raise RuntimeError(f"Cannot read data from {storage_uri}, blob is empty")

        with tempfile.TemporaryFile() as f:
            await client.read(storage_uri, f)
            f.seek(0)
            return await asyncio.get_running_loop().run_in_executor(  # Running in separate thread to not block loop
                None, serializer.deserialize, f, typ
            )
