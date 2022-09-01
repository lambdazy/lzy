import uuid
from typing import Any, Dict, Optional, Type, TypeVar

from lzy._proxy.result import Just, Nothing, Result
from lzy.api.v2.snapshot.snapshot import Snapshot, SnapshotEntry

T = TypeVar("T")  # pylint: disable=invalid-name


class LocalSnapshot(Snapshot):
    def create_entry(
        self, typ: Type, storage_name: Optional[str] = None
    ) -> SnapshotEntry:
        e = SnapshotEntry(str(uuid.uuid4()), typ, "", storage_name)
        self._entry_id_to_entry[e.id] = e
        return e

    def get_data(self, entry_id: str) -> Result[Any]:
        res = self._entry_id_to_value.get(entry_id, Nothing())
        if isinstance(res, Nothing):
            return res
        return Just(res)

    def put_data(self, entry_id: str, data: Any):
        self._entry_id_to_value[entry_id] = data

    def get(self, entry_id: str) -> SnapshotEntry:
        return self._entry_id_to_entry[entry_id]

    def __init__(self):
        self._entry_id_to_value: Dict[str, str] = {}
        self._entry_id_to_entry: Dict[str, SnapshotEntry] = {}
