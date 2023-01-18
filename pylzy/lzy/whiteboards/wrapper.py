import asyncio
import sys
import tempfile
from enum import Enum
from typing import Dict, Union, Any, Iterable, Type

from serialzy.api import SerializerRegistry, Schema, Serializer
from tqdm import tqdm

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, WhiteboardField
from lzy.logs.config import get_color
from lzy.proxy.result import Nothing
from lzy.storage.api import StorageRegistry
from lzy.utils.event_loop import LzyEventLoop


class MissingWhiteboardFieldType:
    pass


MISSING_WHITEBOARD_FIELD = MissingWhiteboardFieldType()


class WhiteboardStatus(Enum):
    CREATED = "CREATED"
    FINALIZED = "FINALIZED"
    UNKNOWN = "UNKNOWN"


class WhiteboardWrapper:
    def __init__(
        self,
        storage_registry: StorageRegistry,
        serializer_registry: SerializerRegistry,
        wb: Whiteboard
    ):
        cli = storage_registry.client(wb.storage.name)

        if cli is None:
            raise RuntimeError(
                f"Storage {wb.storage.name} not found in your registry, available "
                f"{storage_registry.available_storages()}")

        self.__storage = cli

        self.__serializers = serializer_registry
        self.__wb = wb
        self.__fields: Dict[str, Union[WhiteboardField, Any]] = {
            field.name: field for field in wb.fields
        }

    def __getattr__(self, item: str) -> Any:
        var = self.__fields.get(item, Nothing())

        if isinstance(var, Nothing):
            raise AttributeError(f"Whiteboard field {item} not found")

        if isinstance(var, WhiteboardField):
            var = LzyEventLoop.run_async(self.__read_data(var))
            self.__fields[item] = var
            return var

        return var

    @property
    def id(self) -> str:
        return self.__wb.id

    @property
    def name(self) -> str:
        return self.__wb.name

    @property
    def tags(self) -> Iterable[str]:
        return self.__wb.tags

    @property
    def storage_uri(self) -> str:
        return self.__wb.storage.uri

    @property
    def status(self) -> WhiteboardStatus:
        if self.__wb.status == Whiteboard.Status.CREATED:
            return WhiteboardStatus.CREATED
        elif self.__wb.status == Whiteboard.Status.FINALIZED:
            return WhiteboardStatus.FINALIZED
        else:
            return WhiteboardStatus.UNKNOWN

    async def __read_data(self, field: WhiteboardField) -> Any:
        data_scheme = field.scheme
        serializer = self.__serializers.find_serializer_by_data_format(data_scheme.dataFormat)
        if serializer is None:
            raise TypeError(f"Serializer not found for data format {data_scheme.dataFormat}")
        elif not serializer.available():
            raise TypeError(
                f'Serializer for data format {data_scheme.dataFormat} is not available, '
                f'please install {serializer.requirements()}')

        schema = Schema(
            data_format=data_scheme.dataFormat,
            schema_format=data_scheme.schemeFormat,
            schema_content=data_scheme.schemeContent,
            meta=dict(**data_scheme.metadata)
        )

        typ = serializer.resolve(schema)
        storage_uri = f"{self.__wb.storage.uri}/{field.name}"
        exists = await self.__storage.blob_exists(storage_uri)
        if exists:
            return await self.__load_and_deserialize(f"{self.name}.{field.name}", storage_uri, serializer, typ)

        storage_uri_default = f"{self.__wb.storage.uri}/{field.name}.default"
        exists_default = await self.__storage.blob_exists(storage_uri_default)
        if exists_default:
            return await self.__load_and_deserialize(f"{self.name}.{field.name}", storage_uri_default, serializer, typ)

        return MISSING_WHITEBOARD_FIELD

    async def __load_and_deserialize(self, name: str, storage_uri: str, serializer: Serializer, typ: Type) -> Any:
        size = await self.__storage.size_in_bytes(storage_uri)
        with tqdm(total=size, desc=f"Downloading {name}", file=sys.stdout, unit='B', unit_scale=True,
                  unit_divisor=1024, colour=get_color()) as bar:
            with tempfile.TemporaryFile() as f:
                await self.__storage.read(storage_uri, f, progress=lambda x: bar.update(x))  # type: ignore
                f.seek(0)
                # Running in separate thread to not block loop
                return await asyncio.get_running_loop().run_in_executor(None, serializer.deserialize, f, typ)
