import dataclasses
from abc import ABC, abstractmethod
from typing import Any, Optional, Type, TypeVar

from lzy._proxy.result import Result

T = TypeVar("T")  # pylint: disable=invalid-name


@dataclasses.dataclass(frozen=True)
class DataScheme:
    type: str
    scheme_type: str


@dataclasses.dataclass(frozen=True)
class SnapshotEntry:
    id: str
    typ: Type
    storage_url: str
    storage_name: Optional[str] = None
    data_scheme: Optional[DataScheme] = None


class Snapshot(ABC):
    @abstractmethod
    def create_entry(
        self, typ: Type, storage_name: Optional[str] = None
    ) -> SnapshotEntry:
        pass

    @abstractmethod
    def get_data(self, entry_id: str) -> Result[Any]:
        pass

    @abstractmethod
    def put_data(self, entry_id: str, data: Any):
        pass

    @abstractmethod
    def get(self, entry_id: str) -> SnapshotEntry:
        pass
