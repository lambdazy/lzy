import dataclasses
from typing import Any, Dict, Callable, Optional

from lzy.api.whiteboard.api import WhiteboardApi
from lzy.api.utils import is_lazy_proxy

ALREADY_WRAPPED = '_already_wrapped'
def wrap_whiteboard(
    instance: Any,
    whiteboard_api: WhiteboardApi,
    whiteboard_id_getter: Callable[[], Optional[str]],
):
    if not dataclasses.is_dataclass(instance):
        raise RuntimeError("Only dataclasses can be whiteboard")

    if hasattr(instance, ALREADY_WRAPPED):
        return

    fields = dataclasses.fields(instance)
    fields_dict: Dict[str, dataclasses.Field] = {
        field.name: field
        for field in fields
    }

    def __setattr__(self: Any, key: str, value: Any):
        if key not in fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if not is_lazy_proxy(value):
            super(type(instance), self).__setattr__(key, value)
            return

        # pylint: disable=protected-access
        return_entry_id = value._op.return_entry_id()
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
