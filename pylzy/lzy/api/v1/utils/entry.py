from typing import Any

from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id

__entry_id = "__lzy_entry_id__"


def has_entry_id(obj: Any) -> bool:
    if is_lzy_proxy(obj):
        return True
    return hasattr(obj, __entry_id)


def get_entry_id(obj: Any) -> str:
    if not has_entry_id(obj):
        raise ValueError(f'Object {obj} does not have attached entry id')

    if is_lzy_proxy(obj):
        return get_proxy_entry_id(obj)
    return getattr(obj, __entry_id)


def attach_entry_id(obj: Any, entry_id: str) -> None:
    setattr(obj, __entry_id, entry_id)
