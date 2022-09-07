import dataclasses
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type, TypeVar, cast

from lzy.proxy.result import Just, Nothing, Result
from lzy.serialization.api import SerializerRegistry
from lzy.storage.api import AsyncStorageClient, StorageRegistry

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
    def create_entry(self, typ: Type) -> SnapshotEntry:
        # TODO (tomato): add prefixes to entry
        pass

    @abstractmethod
    def get_data(self, entry_id: str) -> Result[Any]:
        pass

    @abstractmethod
    def put_data(self, entry_id: str, data: Any) -> None:
        pass

    @abstractmethod
    def resolve_url(self, entry_id: str) -> Optional[str]:
        return str(uuid.uuid4())

    @abstractmethod
    def get(self, entry_id: str) -> SnapshotEntry:
        pass

    @abstractmethod
    def storage_name(self) -> str:
        pass


class DefaultSnapshot(Snapshot):
    def __init__(
        self, storage_registry: StorageRegistry, serializer_registry: SerializerRegistry
    ):
        if (
            storage_registry.default_storage_name() is None
            or storage_registry.default_client() is None
        ):
            raise ValueError("Cannot initialize snapshot: no default storage")
        self.__serializer_registry = serializer_registry
        self.__storage_client = cast(
            AsyncStorageClient, storage_registry.default_client()
        )
        self.__storage_name = cast(str, storage_registry.default_storage_name())
        self.__entry_id_to_value: Dict[str, str] = {}
        self.__entry_id_to_entry: Dict[str, SnapshotEntry] = {}

        # TODO (tomato): add uploading/downloading from storage

    def create_entry(self, typ: Type) -> SnapshotEntry:
        e = SnapshotEntry(str(uuid.uuid4()), typ, "", self.__storage_name)
        self.__entry_id_to_entry[e.id] = e
        return e

    def get_data(self, entry_id: str) -> Result[Any]:
        res = self.__entry_id_to_value.get(entry_id, Nothing())
        if isinstance(res, Nothing):
            return res
        return Just(res)

    def put_data(self, entry_id: str, data: Any):
        self.__entry_id_to_value[entry_id] = data

    def get(self, entry_id: str) -> SnapshotEntry:
        return self.__entry_id_to_entry[entry_id]

    def resolve_url(self, entry_id: str) -> str:
        return str(uuid.uuid4())

    def storage_name(self) -> str:
        return self.__storage_name
