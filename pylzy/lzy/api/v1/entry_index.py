from typing import Any, Dict

from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id


class EntryIndex:
    __entry_id = "__lzy_entry_id__"

    def __init__(self):
        self.__index: Dict[int, str] = {}

    def add_entry_id(self, obj: Any, entry_id: str) -> None:
        self.__index[id(obj)] = entry_id

    def has_entry_id(self, obj: Any) -> bool:
        if is_lzy_proxy(obj):
            return True
        return id(obj) in self.__index

    def get_entry_id(self, obj: Any) -> str:
        if not self.has_entry_id(obj):
            raise ValueError(f'Object {obj} does not have attached entry id')

        if is_lzy_proxy(obj):
            return get_proxy_entry_id(obj)
        return self.__index[id(obj)]
