from typing import Any, Optional


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class EntryRepository:
    def __init__(self):
        self._entry_id_to_value = {}

    def add(self, entry_id: str, value: Any) -> None:
        self._entry_id_to_value[entry_id] = value

    def get(self, entry_id) -> Optional[Any]:
        if entry_id in self._entry_id_to_value:
            return self._entry_id_to_value[entry_id]
        return None

    def remove(self, entry_id: str) -> Optional[Any]:
        return self._entry_id_to_value.pop(entry_id, None)

    def clear(self) -> None:
        self._entry_id_to_value = {}