import uuid
from typing import Type, Any, TypeVar, Optional

from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.in_mem.entry_repository import EntryRepository
from lzy.serialization.serializer import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


class LocalSnapshot(Snapshot):
    def __init__(self):
        self._id = str(uuid.uuid4())
        self._entry_id_to_value = {}
        self._entry_repository = EntryRepository()

    def id(self) -> str:
        return self._id

    def serializer(self) -> Serializer:
        pass

    def shape(self, wb_type: Type[T]) -> T:
        raise AttributeError("Whiteboards are not supported in local snapshot")

    def get(self, entry_id: str) -> Optional[Any]:
        return self._entry_repository.get(entry_id)

    def put(self, entry_id: str, data: Any) -> None:
        self._entry_repository.add(entry_id, data)

    def finalize(self):
        pass

    def error(self):
        pass
