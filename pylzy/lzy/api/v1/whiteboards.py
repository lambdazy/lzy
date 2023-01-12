import dataclasses
from dataclasses import dataclass
from typing import Optional, Type, Dict, Any, Iterable, Mapping, Set, TYPE_CHECKING

from serialzy.types import get_type

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id, lzy_proxy
from lzy.proxy.result import Just
from lzy.utils.event_loop import LzyEventLoop

if TYPE_CHECKING:
    from lzy.api.v1.workflow import LzyWorkflow

WB_NAME_FIELD_NAME = "__lzy_wb_name__"


@dataclass
class DeclaredWhiteboardMeta:
    name: str


def whiteboard_(cls: Type, name: str):
    if not name:
        raise TypeError("tags attribute must be specified")

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


class WritableWhiteboard:
    __internal_fields = {
        "_WritableWhiteboard__fields_dict", "_WritableWhiteboard__fields_assigned",
        "_WritableWhiteboard__model", "_WritableWhiteboard__workflow", "_WritableWhiteboard__fields",
    }

    def __init__(
        self,
        instance: Any,
        model: Whiteboard,
        workflow: "LzyWorkflow",
        fields: Mapping[str, Any]
    ):
        self.__fields_dict: Dict[str, dataclasses.Field] = {
            field.name: field for field in dataclasses.fields(instance)
        }
        self.__fields_assigned: Set[str] = set()
        self.__model = model
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

        storage_uri = f"{self.__model.storage.uri}/{key}"
        proxy = is_lzy_proxy(value)
        if proxy:
            entry = self.__workflow.snapshot.get(get_proxy_entry_id(value))
            if entry.id in self.__workflow.filled_entry_ids:
                # TODO (tomato): copy data to whiteboard uri
                pass
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
        if item not in self.__fields:
            raise AttributeError(f"Whiteboard has no field {item}")
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
