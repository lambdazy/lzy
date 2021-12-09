import types
from typing import Any, Dict, Callable
from lzy.api.whiteboard.api import WhiteboardApi
import dataclasses


def wrap_whiteboard(instance: Any, whiteboard_api: WhiteboardApi, whiteboard_id_getter: Callable[[], str]):

    from lzy.api import is_lazy_proxy

    if not dataclasses.is_dataclass(instance):
        raise RuntimeError("Only dataclasses can be whiteboard")
    fields = dataclasses.fields(instance)
    fields_dict: Dict[str, dataclasses.Field] = {field.name: field for field in fields}

    def __setattr__(self: Any, key: str, value: Any):
        if key not in fields_dict:
            raise AttributeError(f'No such attribute')
        if not is_lazy_proxy(value):
            raise AttributeError(f'Value must be @op')
        return_entry_id = value._op.return_entry_id()
        if return_entry_id is not None:
            whiteboard_api.link(whiteboard_id_getter(), key, return_entry_id)
        object.__setattr__(self, key, value)

    type(instance).__setattr__ = types.MethodType(__setattr__, instance)
