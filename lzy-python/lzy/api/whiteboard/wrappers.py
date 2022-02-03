import dataclasses
from typing import Any, Callable, Optional, Dict

from lzy.api.utils import is_lazy_proxy
from lzy.api.whiteboard import WhiteboardApi, is_whiteboard, WhiteboardDescription

ALREADY_WRAPPED = '_already_wrapped_whiteboard'
ALREADY_WRAPPED_READY = '_already_wrapped_ready_whiteboard'


def wrap_whiteboard(
        instance: Any,
        whiteboard_api: WhiteboardApi,
        whiteboard_id_getter: Callable[[], Optional[str]],
):
    if not is_whiteboard(instance):
        raise RuntimeError("Whiteboard must be a dataclass and have a @whiteboard decorator")

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

    setattr(instance, "id", whiteboard_id_getter)
    setattr(instance, ALREADY_WRAPPED, True)
    type(instance).__setattr__ = __setattr__  # type: ignore


def wrap_whiteboard_for_read(instance: Any, wd: WhiteboardDescription):
    if hasattr(instance, ALREADY_WRAPPED_READY):
        return

    setattr(instance, "id", wd.id)
    setattr(instance, "status", wd.status)
    setattr(instance, ALREADY_WRAPPED_READY, True)
