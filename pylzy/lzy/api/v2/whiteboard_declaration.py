import dataclasses
from dataclasses import dataclass
from typing import Optional, Type

from lzy.serialization.api import Schema

WB_NAMESPACE_FIELD_NAME = "__lzy_wb_namespace__"
WB_NAME_FIELD_NAME = "__lzy_wb_name__"


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


@dataclass
class WhiteboardField:
    name: str
    url: Optional[str] = None
    data_scheme: Optional[Schema] = None


@dataclass
class WhiteboardInstanceMeta:
    id: str
