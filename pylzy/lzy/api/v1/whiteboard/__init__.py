import dataclasses
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from pure_protobuf.dataclasses_ import message  # type: ignore

from lzy.api.v1.servant.channel_manager import ChannelManager
from lzy.api.v1.servant.model.slot import DataSchema, pickle_type
from lzy.api.v1.utils import is_lazy_proxy
from lzy.api.v1.whiteboard.model import (
    EntryIdGenerator,
    WhiteboardApi,
    WhiteboardDescription,
)
from lzy.serialization.api import FileSerializer

ALREADY_WRAPPED = "_already_wrapped_whiteboard"
ALREADY_WRAPPED_READY = "_already_wrapped_ready_whiteboard"

WB_ID_GETTER_NAME = "__id_getter__"
LZY_FIELDS_ASSIGNED = "__lzy_fields_assigned__"


def whiteboard(tags: List[str], namespace="default"):
    def wrap(cls):
        return whiteboard_(cls, namespace, tags)

    return wrap


def whiteboard_(cls, namespace, tags):
    if not isinstance(tags, list) or not all(isinstance(elem, str) for elem in tags):
        raise TypeError("tags attribute is required to be a list of strings")

    if not tags:
        raise TypeError("tags attribute must be specified")

    if namespace is not None and not isinstance(namespace, str):
        raise TypeError("namespace attribute is required to be a string")

    cls.LZY_WB_NAMESPACE = namespace
    cls.LZY_WB_TAGS = tags
    cls.__id__ = property(lambda self: getattr(self, WB_ID_GETTER_NAME)())
    return cls


def is_whiteboard(obj: Any) -> bool:
    if obj is None:
        return False

    cls = obj if isinstance(obj, type) else type(obj)
    return (
        hasattr(cls, "LZY_WB_NAMESPACE")
        and hasattr(cls, "LZY_WB_TAGS")
        and dataclasses.is_dataclass(cls)
    )


def check_whiteboard(obj: Any) -> None:
    if not is_whiteboard(obj):
        raise ValueError(
            "Whiteboard must be a dataclass and have a @whiteboard decorator"
        )


def view(func):
    func.LZY_WB_VIEW_DECORATOR = "view_deco"
    return func


def wrap_whiteboard(
    instance: Any,
    whiteboard_api: WhiteboardApi,
    whiteboard_id_getter: Callable[[], Optional[str]],
    channel_manager: ChannelManager,
    serializer: FileSerializer,
    entry_id_generator: EntryIdGenerator,
):
    check_whiteboard(instance)
    if hasattr(instance, ALREADY_WRAPPED):
        return

    fields_dict: Dict[str, dataclasses.Field] = {
        field.name: field for field in dataclasses.fields(instance)
    }

    fields_assigned: Set[str] = set()

    def dump(path: Path, value: Any):
        with path.open("wb") as handle:
            serializer.serialize_to_file(value, handle)
            handle.flush()
            os.fsync(handle.fileno())

    def __setattr__(self: Any, key: str, value: Any):
        if not hasattr(self, ALREADY_WRAPPED):
            super(type(self), self).__setattr__(key, value)
            return

        if key not in fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if key in fields_assigned:
            raise AttributeError("Whiteboard field can be assigned only once")

        whiteboard_id = whiteboard_id_getter()
        if whiteboard_id is None:
            raise RuntimeError("Cannot get whiteboard id")

        if is_lazy_proxy(value):
            # noinspection PyProtectedMember
            entry_id = value._op.entry_id  # pylint: disable=protected-access
            if entry_id is None:
                raise RuntimeError("Cannot get entry_id from op")
        else:
            entry_id = entry_id_generator.generate("/wb/field/" + key)
            data_scheme = DataSchema.generate_schema(type(value))
            dump(channel_manager.out_slot(entry_id, data_scheme), value)

        whiteboard_api.link(whiteboard_id, key, entry_id)
        fields_assigned.add(key)

        # interesting fact: super() doesn't work in outside-defined functions
        # as it works in methods of classes
        # and we actually need to pass class and instance here
        return super(type(instance), self).__setattr__(key, value)

    setattr(instance, WB_ID_GETTER_NAME, whiteboard_id_getter)
    setattr(instance, LZY_FIELDS_ASSIGNED, fields_assigned)
    setattr(instance, ALREADY_WRAPPED, True)
    type(instance).__setattr__ = __setattr__  # type: ignore


def wrap_whiteboard_for_read(instance: Any, wd: WhiteboardDescription):
    check_whiteboard(instance)
    if hasattr(instance, ALREADY_WRAPPED_READY):
        return

    setattr(instance, WB_ID_GETTER_NAME, lambda: wd.id)
    setattr(instance, "__status__", wd.status)
    setattr(instance, ALREADY_WRAPPED_READY, True)
