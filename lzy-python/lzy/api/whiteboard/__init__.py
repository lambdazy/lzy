import dataclasses
from typing import List, Any, Callable, Optional, Dict

from lzy.api.utils import is_lazy_proxy
from lzy.api.whiteboard.model import WhiteboardApi, WhiteboardDescription

ALREADY_WRAPPED = '_already_wrapped_whiteboard'
ALREADY_WRAPPED_READY = '_already_wrapped_ready_whiteboard'

WB_ID_GETTER_NAME = '__id_getter__'


def whiteboard(tags: List[str], namespace=None):
    def wrap(cls):
        return whiteboard_(cls, namespace, tags)

    return wrap


def whiteboard_(cls, namespace, tags):
    if not isinstance(tags, list) or not all(isinstance(elem, str) for elem in tags):
        raise TypeError('tags attribute is required to be a list of strings')
    if not tags:
        raise TypeError('tags attribute must be specified')
    if namespace is not None and not isinstance(namespace, str):
        raise TypeError('namespace attribute is required to be a string')
    cls.LZY_WB_NAMESPACE = namespace
    cls.LZY_WB_TAGS = tags
    cls.__id__ = property(lambda self: getattr(self, WB_ID_GETTER_NAME)())
    return cls


def is_whiteboard(obj: Any) -> bool:
    if obj is None:
        return False

    cls = obj if isinstance(obj, type) else type(obj)
    return hasattr(cls, 'LZY_WB_NAMESPACE') and hasattr(cls, 'LZY_WB_TAGS') and dataclasses.is_dataclass(cls)


def check_whiteboard(obj: Any) -> None:
    if not is_whiteboard(obj):
        raise ValueError("Whiteboard must be a dataclass and have a @whiteboard decorator")


def view(func):
    func.LZY_WB_VIEW_DECORATOR = 'view_deco'
    return func


def wrap_whiteboard(
        instance: Any,
        whiteboard_api: WhiteboardApi,
        whiteboard_id_getter: Callable[[], Optional[str]],
):
    check_whiteboard(instance)
    if hasattr(instance, ALREADY_WRAPPED):
        return

    fields = dataclasses.fields(instance)
    fields_dict: Dict[str, dataclasses.Field] = {
        field.name: field
        for field in fields
    }

    def __setattr__(self: Any, key: str, value: Any):
        if not hasattr(self, ALREADY_WRAPPED):
            super(type(self), self).__setattr__(key, value)
            return

        if key not in fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if not is_lazy_proxy(value):
            raise ValueError('Only @op return values can be assigned to whiteboard')

        prev_value = super(type(instance), self).__getattribute__(key)
        if is_lazy_proxy(prev_value):
            raise ValueError('Whiteboard field can be assigned only once')

        # noinspection PyProtectedMember
        return_entry_id = value._op.return_entry_id()  # pylint: disable=protected-access
        whiteboard_id = whiteboard_id_getter()
        if return_entry_id is None or whiteboard_id is None:
            raise RuntimeError("Cannot get entry_id from op")

        whiteboard_api.link(whiteboard_id, key, return_entry_id)
        # interesting fact: super() doesn't work in outside-defined functions
        # as it works in methods of classes
        # and we actually need to pass class and instance here
        super(type(instance), self).__setattr__(key, value)

    setattr(instance, WB_ID_GETTER_NAME, whiteboard_id_getter)
    setattr(instance, ALREADY_WRAPPED, True)
    type(instance).__setattr__ = __setattr__  # type: ignore


def wrap_whiteboard_for_read(instance: Any, wd: WhiteboardDescription):
    check_whiteboard(instance)
    if hasattr(instance, ALREADY_WRAPPED_READY):
        return

    setattr(instance, WB_ID_GETTER_NAME, lambda: wd.id)
    setattr(instance, "__status__", wd.status)
    setattr(instance, ALREADY_WRAPPED_READY, True)
