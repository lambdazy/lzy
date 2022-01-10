import dataclasses
from typing import Any, Dict, Callable, Optional

from lzy.api.whiteboard.api import WhiteboardApi


def wrap_whiteboard(instance: Any, whiteboard_api: WhiteboardApi,
                    whiteboard_id_getter: Callable[[], Optional[str]]):

    from lzy.api import is_lazy_proxy

    if not dataclasses.is_dataclass(instance):
        raise RuntimeError("Only dataclasses can be whiteboard")
    fields = dataclasses.fields(instance)
    fields_dict: Dict[str, dataclasses.Field] = {field.name: field for field in fields}

    def __setattr__(self: Any, key: str, value: Any):
        if key not in fields_dict:
            raise AttributeError(f'No such attribute')
        if not is_lazy_proxy(value):
            object.__setattr__(self, key, value)
            return
        return_entry_id = value._op.return_entry_id()
        whiteboard_id = whiteboard_id_getter()
        if return_entry_id is not None and whiteboard_id is not None:
            whiteboard_api.link(whiteboard_id, key, return_entry_id)
        else:
            raise RuntimeError("Cannot get entry_id from op")
        object.__setattr__(self, key, value)

    object.__setattr__(instance, 'id', whiteboard_id_getter)
    type(instance).__setattr__ = __setattr__  # type: ignore
