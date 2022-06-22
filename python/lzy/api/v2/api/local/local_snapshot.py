import uuid
from typing import Type, Any, TypeVar, Optional

from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.serialization.serializer import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


class LocalSnapshot(Snapshot):
    def __init__(self):
        self._id: str = str(uuid.uuid4())
        self._entry_id_to_value = {}

    def id(self) -> str:
        return self._id

    def serializer(self) -> Serializer:
        pass

    def shape(self, wb_type: Type[T]) -> T:
        raise AttributeError("Whiteboards are not supported in local snapshot")

    def get(self, entry_id: str) -> Optional[Any]:
        if entry_id in self._entry_id_to_value:
            return self._entry_id_to_value[entry_id]
        return None

    def put(self, entry_id: str, data: Any) -> None:
        self._entry_id_to_value[entry_id] = data

    def finalize(self):
        pass

    def error(self):
        pass
