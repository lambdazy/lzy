import dataclasses
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, Type, cast, BinaryIO, Set

from serialzy.api import Schema, SerializerRegistry
from tqdm import tqdm

from lzy.logs.config import get_logger, get_color
from lzy.proxy.result import Just, Nothing, Result
from lzy.storage.api import AsyncStorageClient

_LOG = get_logger(__name__)


@dataclasses.dataclass(frozen=True)
class DataScheme:
    type: str
    scheme_type: str


@dataclasses.dataclass(frozen=True)
class SnapshotEntry:
    id: str
    name: str
    typ: Type
    storage_uri: str
    storage_name: str
    data_scheme: Schema


class Snapshot(ABC):  # pragma: no cover
    @abstractmethod
    def create_entry(self, name: str, typ: Type, storage_uri: str) -> SnapshotEntry:
        pass

    @abstractmethod
    def update_entry(self, entry_id: str, storage_uri: str) -> None:
        pass

    @abstractmethod
    async def get_data(self, entry_id: str) -> Result[Any]:
        pass

    @abstractmethod
    async def put_data(self, entry_id: str, data: Any) -> None:
        pass

    @abstractmethod
    def get(self, entry_id: str) -> SnapshotEntry:
        pass


class DefaultSnapshot(Snapshot):
    def __init__(self, serializer_registry: SerializerRegistry, storage_client: AsyncStorageClient, storage_name: str):
        self.__serializer_registry = serializer_registry
        self.__storage_client = storage_client
        self.__storage_name = storage_name
        self.__entry_id_to_entry: Dict[str, SnapshotEntry] = {}
        self.__filled_entries: Set[str] = set()

    def create_entry(self, name: str, typ: Type, storage_uri: str) -> SnapshotEntry:
        eid = str(uuid.uuid4())
        serializer_by_type = self.__serializer_registry.find_serializer_by_type(typ)
        if serializer_by_type is None:
            raise TypeError(f'Cannot find serializer for type {typ}')
        elif not serializer_by_type.available():
            raise TypeError(
                f'Serializer for type {typ} is not available, please install {serializer_by_type.requirements()}')

        schema = serializer_by_type.schema(typ)
        e = SnapshotEntry(eid, name, typ, storage_uri, self.__storage_name, data_scheme=schema)
        self.__entry_id_to_entry[e.id] = e
        _LOG.debug(f"Created entry {e}")
        return e

    def update_entry(self, entry_id: str, storage_uri: str) -> None:
        if entry_id in self.__filled_entries:
            raise ValueError(f"Cannot update entry {entry_id}: data has been already uploaded")

        prev = self.get(entry_id)
        self.__entry_id_to_entry[entry_id] = SnapshotEntry(entry_id, prev.name, prev.typ, storage_uri,
                                                           prev.storage_name, prev.data_scheme)

    async def get_data(self, entry_id: str) -> Result[Any]:
        _LOG.debug(f"Getting data for entry {entry_id}")
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Entry with id={entry_id} does not exist")

        exists = await self.__storage_client.blob_exists(entry.storage_uri)
        if not exists:
            return Nothing()

        with tempfile.NamedTemporaryFile() as f:
            size = await self.__storage_client.size_in_bytes(entry.storage_uri)
            with tqdm(total=size, desc=f"Downloading {entry.name}", file=sys.stdout, unit='B', unit_scale=True,
                      unit_divisor=1024, colour=get_color()) as bar:
                await self.__storage_client.read(entry.storage_uri, cast(BinaryIO, f),
                                                 progress=lambda x: bar.update(x))
                f.seek(0)
                res = self.__serializer_registry.find_serializer_by_type(entry.typ).deserialize(cast(BinaryIO, f))
                return Just(res)

    async def put_data(self, entry_id: str, data: Any) -> None:
        _LOG.debug(f"Putting data for entry {entry_id}")
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Entry with id={entry_id} does not exist")

        with tempfile.NamedTemporaryFile() as f:
            _LOG.debug(f"Serializing {entry.name}...")
            serializer = self.__serializer_registry.find_serializer_by_type(entry.typ)
            serializer.serialize(data, f)
            length = f.tell()
            f.seek(0)

            with tqdm(total=length, desc=f"Uploading {entry.name}", file=sys.stdout, unit='B', unit_scale=True,
                      unit_divisor=1024, colour=get_color()) as bar:
                await self.__storage_client.write(entry.storage_uri, cast(BinaryIO, f),
                                                  progress=lambda x: bar.update(x))

        self.__filled_entries.add(entry_id)

    def get(self, entry_id: str) -> SnapshotEntry:
        return self.__entry_id_to_entry[entry_id]
