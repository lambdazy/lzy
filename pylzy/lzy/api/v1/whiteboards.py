import dataclasses
import datetime
import uuid
from dataclasses import dataclass
from typing import Optional, Type, Dict, Any, Iterable, Set, TYPE_CHECKING, Sequence

# noinspection PyPackageRequirements
from google.protobuf.timestamp_pb2 import Timestamp
from serialzy.api import Schema
from serialzy.types import get_type

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard, WhiteboardField, Storage
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id, lzy_proxy
from lzy.proxy.result import Just
from lzy.utils.event_loop import LzyEventLoop

if TYPE_CHECKING:  # pragma: no cover
    from lzy.api.v1.workflow import LzyWorkflow

WB_NAME_FIELD_NAME = "__lzy_wb_name__"


@dataclass
class DeclaredWhiteboardMeta:
    name: str


def whiteboard_(cls: Type, name: str):
    if not name:
        raise ValueError("name attribute must be specified")

    if not isinstance(name, str):
        raise TypeError("name attribute is required to be a string")

    setattr(cls, WB_NAME_FIELD_NAME, name)
    return cls


def is_whiteboard(typ: Type) -> bool:
    return (
        hasattr(typ, WB_NAME_FIELD_NAME)
        and dataclasses.is_dataclass(typ)
    )


def fetch_whiteboard_meta(typ: Type) -> Optional[DeclaredWhiteboardMeta]:
    if not is_whiteboard(typ):
        return None
    return DeclaredWhiteboardMeta(getattr(typ, WB_NAME_FIELD_NAME))


def build_scheme(data_scheme: Schema) -> DataScheme:
    return DataScheme(
        dataFormat=data_scheme.data_format,
        schemeFormat=data_scheme.schema_format,
        schemeContent=data_scheme.schema_content
        if data_scheme.schema_content else "",
        metadata=data_scheme.meta
    )


class WritableWhiteboard:
    __internal_fields = {
        "_WritableWhiteboard__fields_dict", "_WritableWhiteboard__fields_assigned",
        "_WritableWhiteboard__model", "_WritableWhiteboard__workflow", "_WritableWhiteboard__fields",
    }

    def __init__(
        self,
        typ: Type,
        tags: Sequence[str],
        workflow: "LzyWorkflow"
    ):
        self.__workflow = workflow
        declaration_meta = fetch_whiteboard_meta(typ)
        if declaration_meta is None:
            raise TypeError(
                f"Whiteboard class should be annotated with both @whiteboard and @dataclass"
            )

        whiteboard_id = str(uuid.uuid4())
        storage_prefix = f"whiteboards/{declaration_meta.name}/{whiteboard_id}"
        whiteboard_uri = f"{workflow.owner.storage_uri}/{storage_prefix}"

        # noinspection PyDataclass
        declared_fields = dataclasses.fields(typ)
        fields = []
        data_to_load = []
        defaults = {}

        for field in declared_fields:
            serializer = workflow.owner.serializer_registry.find_serializer_by_type(field.type)
            if serializer is None:
                raise TypeError(f'Cannot find serializer for type {typ}')
            elif not serializer.available():
                raise TypeError(
                    f'Serializer for type {field.type} is not available, please install {serializer.requirements()}')
            elif not serializer.stable():
                raise TypeError(
                    f'Variables of type {field.type} cannot be assigned on whiteboard'
                    f' because we cannot serialize them in a portable format. '
                    f'See https://github.com/lambdazy/serialzy for details.')

            if field.default != dataclasses.MISSING:
                entry = workflow.snapshot.create_entry(declaration_meta.name + "." + field.name, field.type,
                                                       f"{whiteboard_uri}/{field.name}.default")
                data_to_load.append(workflow.snapshot.put_data(entry.id, field.default))
                defaults[field.name] = lzy_proxy(entry.id, (field.type,), workflow, Just(field.default))

            fields.append(WhiteboardField(name=field.name, scheme=build_scheme(serializer.schema(field.type))))

        LzyEventLoop.gather(*data_to_load)
        # noinspection PyDataclass
        self.__fields_dict: Dict[str, dataclasses.Field] = {field.name: field for field in dataclasses.fields(typ)}
        self.__fields: Dict[str, Any] = {}
        self.__fields.update(defaults)

        now = datetime.datetime.now()
        timestamp = Timestamp()
        timestamp.FromDatetime(now)
        whiteboard = Whiteboard(
            id=whiteboard_id,
            name=declaration_meta.name,
            tags=tags,
            fields=fields,
            storage=Storage(name=workflow.owner.storage_name, uri=whiteboard_uri),
            namespace="default",
            status=Whiteboard.Status.CREATED,
            createdAt=timestamp
        )
        LzyEventLoop.run_async(workflow.owner.whiteboard_manager.write_meta(whiteboard, whiteboard_uri))

        self.__model = whiteboard
        self.__fields_assigned: Set[str] = set()

    def __setattr__(self, key: str, value: Any):
        if key in WritableWhiteboard.__internal_fields:  # To complete constructor
            super(WritableWhiteboard, self).__setattr__(key, value)
            return

        if key not in self.__fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if key in self.__fields_assigned:
            raise AttributeError("Whiteboard field can be assigned only once")

        storage_uri = f"{self.__model.storage.uri}/{key}"
        proxy = is_lzy_proxy(value)
        if proxy:
            entry = self.__workflow.snapshot.get(get_proxy_entry_id(value))
            if entry.id in self.__workflow.filled_entry_ids:
                LzyEventLoop.run_async(self.__workflow.owner.storage_client.copy(entry.storage_uri, storage_uri))
            else:
                self.__workflow.snapshot.update_entry(entry.id, storage_uri)
        else:
            entry = self.__workflow.snapshot.create_entry(self.__model.name + "." + key, get_type(value), storage_uri)
            LzyEventLoop.run_async(self.__workflow.snapshot.put_data(entry_id=entry.id, data=value))
            # noinspection PyTypeChecker
            # TODO: there is no need to create lazy proxy from value. We only need to attach entry id
            value = lzy_proxy(entry.id, (type(value),), self.__workflow, Just(value))

        self.__fields_assigned.add(key)
        self.__fields[key] = value

    def __getattr__(self, item: str) -> Any:
        if item not in self.__fields_dict:
            raise AttributeError(f"No such attribute: {item}")
        elif item not in self.__fields:
            raise AttributeError(f"Whiteboard field {item} is not assigned")
        return self.__fields[item]

    @property
    def id(self) -> str:
        return self.__model.id

    @property
    def name(self) -> str:
        return self.__model.name

    @property
    def tags(self) -> Iterable[str]:
        return self.__model.tags

    @property
    def storage_uri(self) -> str:
        return self.__model.storage.uri
