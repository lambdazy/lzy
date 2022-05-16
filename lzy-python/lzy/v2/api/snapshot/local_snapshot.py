import uuid
from typing import Type, Any, TypeVar, Optional

from lzy.v2.api.snapshot.snapshot import Snapshot

T = TypeVar("T")  # pylint: disable=invalid-name


class LocalSnapshot(Snapshot):
    def __init__(self):
        self._id = str(uuid.uuid4())
        self._entry_id_to_value = {}

    def id(self) -> str:
        return self._id

    def shape(self, wb_type: Type[T]) -> T:
        raise AttributeError("Whiteboards are not supported in local snapshot")

    def get(self, entry_id: str) -> Optional[Any]:
        # TODO(lsunko) add implementation
        pass

    def silent(self) -> None:
        pass

    def finalize(self):
        pass

    def error(self):
        pass
