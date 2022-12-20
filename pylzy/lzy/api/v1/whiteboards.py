import asyncio
import dataclasses
import tempfile
from dataclasses import dataclass
from typing import Optional, Type, Dict, Union, Any, Iterable, Mapping, Set, TYPE_CHECKING

from serialzy.api import SerializerRegistry, Schema
from serialzy.types import get_type

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, WhiteboardField
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id, materialized, lzy_proxy
from lzy.proxy.result import Nothing, Just
from lzy.storage.api import StorageRegistry, AsyncStorageClient
from lzy.utils.event_loop import LzyEventLoop
from lzy.whiteboards.api import WhiteboardInstanceMeta

if TYPE_CHECKING:
    from lzy.api.v1.workflow import LzyWorkflow

WB_NAMESPACE_FIELD_NAME = "__lzy_wb_namespace__"
WB_NAME_FIELD_NAME = "__lzy_wb_name__"


@dataclasses.dataclass
class WbRef:
    whiteboard_id: str
    field_name: str


@dataclass
class DeclaredWhiteboardMeta:
    namespace: str
    name: str


def whiteboard_(cls: Type, namespace: str, name: str):
    if not namespace:
        raise TypeError("namespace attribute must be specified")

    if not isinstance(namespace, str):
        raise TypeError("namespace attribute is required to be a string")

    if not name:
        raise TypeError("tags attribute must be specified")

    if not isinstance(name, str):
        raise TypeError("name attribute is required to be a string")

    setattr(cls, WB_NAMESPACE_FIELD_NAME, namespace)
    setattr(cls, WB_NAME_FIELD_NAME, name)
    return cls


def is_whiteboard(typ: Type) -> bool:
    return (
            hasattr(typ, WB_NAMESPACE_FIELD_NAME)
            and hasattr(typ, WB_NAME_FIELD_NAME)
            and dataclasses.is_dataclass(typ)
    )


def fetch_whiteboard_meta(typ: Type) -> Optional[DeclaredWhiteboardMeta]:
    if not is_whiteboard(typ):
        return None
    return DeclaredWhiteboardMeta(
        getattr(typ, WB_NAMESPACE_FIELD_NAME), getattr(typ, WB_NAME_FIELD_NAME)
    )


class ReadOnlyWhiteboard:
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

    async def __read_data(self, field: WhiteboardField, client: AsyncStorageClient) -> Any:
        if field.status != WhiteboardField.FINALIZED:
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
            await client.read(storage_uri, f)  # type: ignore
            f.seek(0)
            return await asyncio.get_running_loop().run_in_executor(  # Running in separate thread to not block loop
                None, serializer.deserialize, f, typ
            )


class WritableWhiteboard:
    __internal_fields = {
        "_WritableWhiteboard__fields_dict", "_WritableWhiteboard__fields_assigned",
        "_WritableWhiteboard__whiteboard_meta", "_WritableWhiteboard__workflow", "_WritableWhiteboard__fields",
    }

    def __init__(
            self,
            instance: Any,
            whiteboard_meta: WhiteboardInstanceMeta,
            workflow: "LzyWorkflow",
            fields: Mapping[str, Any]
    ):
        self.__fields_dict: Dict[str, dataclasses.Field] = {
            field.name: field for field in dataclasses.fields(instance)
        }
        self.__fields_assigned: Set[str] = set()
        self.__whiteboard_meta = whiteboard_meta
        self.__workflow = workflow
        self.__fields: Dict[str, Any] = {}
        self.__fields.update(fields)

    def __setattr__(self, key: str, value: Any):
        if key in WritableWhiteboard.__internal_fields:  # To complete constructor
            super(WritableWhiteboard, self).__setattr__(key, value)
            return

        if key not in self.__fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if key in self.__fields_assigned:
            raise AttributeError("Whiteboard field can be assigned only once")

        whiteboard_id = self.__whiteboard_meta.id

        proxy = is_lzy_proxy(value)
        if proxy:
            entry_id = get_proxy_entry_id(value)
            entry = self.__workflow.snapshot.get(entry_id)
        else:
            entry = self.__workflow.snapshot.create_entry(get_type(value))

        serializer = self.__workflow.owner.serializer.find_serializer_by_type(entry.typ)
        if not serializer.available():
            raise ValueError(
                f'Serializer for type {entry.typ} is not available, please install {serializer.requirements()}')
        if not serializer.stable():
            raise ValueError(
                f'Variables of type {entry.typ} cannot be assigned on whiteboard'
                f' because we cannot serialize them in a portable format. '
                f'See https://github.com/lambdazy/serialzy for details.')

        if proxy:
            if materialized(value):
                LzyEventLoop.run_async(self.__workflow.owner.whiteboard_client.link(
                    whiteboard_id, key, entry.storage_url, entry.data_scheme
                ))
            else:
                self.__workflow.add_whiteboard_link(entry.storage_url, WbRef(whiteboard_id, key))
        else:
            LzyEventLoop.run_async(self.__workflow.snapshot.put_data(entry_id=entry.id, data=value))
            # noinspection PyTypeChecker
            # TODO: there is no need to create lazy proxy from value. We only need to attach entry id
            value = lzy_proxy(entry.id, type(value), self.__workflow, Just(value))
            LzyEventLoop.run_async(self.__workflow.owner.whiteboard_client.link(
                whiteboard_id, key, entry.storage_url, entry.data_scheme
            ))

        self.__fields_assigned.add(key)
        self.__fields[key] = value

    def __getattr__(self, item: str) -> Any:
        if item not in self.__fields:
            raise AttributeError(f"Whiteboard has no field {item}")
        return self.__fields[item]

    @property
    def id(self) -> str:
        return self.__whiteboard_meta.id

    @property
    def name(self) -> str:
        return self.__whiteboard_meta.name

    @property
    def tags(self) -> Iterable[str]:
        return self.__whiteboard_meta.tags
