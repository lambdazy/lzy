import dataclasses
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from serialzy.api import Schema, SerializerRegistry
from tqdm import tqdm
from typing import Any, Dict, Type, cast, BinaryIO, Set, List, Optional

from lzy.api.v1.utils.hashing import HashingIO
from lzy.logs.config import get_logger, get_syslog_color
from lzy.proxy.result import Either, Absence, Result
from lzy.storage.api import AsyncStorageClient

_LOG = get_logger(__name__)


@dataclasses.dataclass(frozen=True)
class DataScheme:
    type: str
    scheme_type: str


@dataclasses.dataclass
class SnapshotEntry:
    id: str
    name: str
    typ: Type
    data_scheme: Schema
    storage_name: str
    _storage_uri: Optional[str] = None
    _data_hash: Optional[str] = None

    @property
    def storage_uri(self) -> str:
        if self._storage_uri is None:
            raise ValueError(f"Storage uri for snapshot entry with id={self.id} is not set")
        return cast(str, self._storage_uri)

    @storage_uri.setter
    def storage_uri(self, uri: str) -> None:
        self._storage_uri = uri

    @property
    def data_hash(self) -> str:
        if self._data_hash is None:
            raise ValueError(f"Data hash for snapshot entry {self.id} is not set")
        return cast(str, self._data_hash)

    @data_hash.setter
    def data_hash(self, hsh: str) -> None:
        self._data_hash = hsh


class Snapshot(ABC):  # pragma: no cover
    @abstractmethod
    def create_entry(self, name: str, typ: Type) -> SnapshotEntry:
        pass

    @abstractmethod
    async def get_data(self, entry_id: str) -> Either[Any]:
        pass

    @abstractmethod
    async def put_data(self, entry_id: str, data: Any) -> None:
        pass

    @abstractmethod
    async def copy_data(self, from_entry_id: str, to_uri: str) -> None:
        pass

    @abstractmethod
    def get(self, entry_id: str) -> SnapshotEntry:
        pass


class DefaultSnapshot(Snapshot):
    def __init__(
            self,
            serializer_registry: SerializerRegistry,
            storage_uri: str,
            storage_client: AsyncStorageClient,
            storage_name: str
    ):
        self.__serializer_registry = serializer_registry
        self.__storage_client = storage_client
        self.__storage_name = storage_name
        self.__storage_uri = storage_uri
        self.__entry_id_to_entry: Dict[str, SnapshotEntry] = {}
        self.__filled_entries: Set[str] = set()
        self.__copy_queue: Dict[str, List[str]] = dict()

    def create_entry(self, name: str, typ: Type) -> SnapshotEntry:
        eid = str(uuid.uuid4())
        serializer_by_type = self.__serializer_registry.find_serializer_by_type(typ)
        if serializer_by_type is None:
            raise TypeError(f'Cannot find serializer for type {typ}')
        elif not serializer_by_type.available():
            raise TypeError(
                f'Serializer for type {typ} is not available, please install {serializer_by_type.requirements()}')

        data_scheme = serializer_by_type.schema(typ)
        e = SnapshotEntry(eid, name, typ, data_scheme, self.__storage_name)
        self.__entry_id_to_entry[e.id] = e
        _LOG.debug("Created entry %r", e)
        return e

    async def get_data(self, entry_id: str) -> Either[Any]:
        _LOG.debug("Getting data for entry %s", entry_id)
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Entry with id={entry_id} does not exist")

        try:
            storage_uri = entry.storage_uri
        except ValueError as e:
            _LOG.debug("Error while getting data for entry %s: %r", entry_id, e)
            return Absence(e)

        exists = await self.__storage_client.blob_exists(storage_uri)
        if not exists:
            _LOG.debug("Cannot find data for entry with id=%s by uri=%s", entry_id, storage_uri)
            return Absence(ValueError(f"Cannot find blob in storage by uri={storage_uri}"))

        with tempfile.NamedTemporaryFile() as f:
            size = await self.__storage_client.size_in_bytes(storage_uri)
            with tqdm(total=size, desc=f"Downloading {entry.name}", file=sys.stdout, unit='B', unit_scale=True,
                      unit_divisor=1024, colour=get_syslog_color()) as bar:

                def progress(update: int, restart: bool):
                    if restart:
                        bar.reset()
                    else:
                        bar.update(update)

                await self.__storage_client.read(storage_uri, cast(BinaryIO, f), progress=progress)
                f.seek(0)
                res = self.__serializer_registry.find_serializer_by_type(entry.typ).deserialize(cast(BinaryIO, f))
                return Result(res)

    async def put_data(self, entry_id: str, data: Any) -> None:
        _LOG.debug("Attempt putting data for entry %s", entry_id)
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Entry with id={entry_id} does not exist")

        with HashingIO(tempfile.NamedTemporaryFile()) as f:
            _LOG.debug("Serializing and calculating data hash of %s...", entry.name)
            serializer = self.__serializer_registry.find_serializer_by_type(entry.typ)
            serializer.serialize(data, f)
            length = f.tell()
            f.seek(0)

            self.__entry_id_to_entry[entry_id].data_hash = f.md5
            entry.storage_uri = self.__storage_uri + f.md5

            exists = await self.__storage_client.blob_exists(entry.storage_uri)
            if not exists:
                _LOG.debug("Upload data for entry %s", entry_id)
                with tqdm(total=length, desc=f"Uploading {entry.name}", file=sys.stdout, unit='B', unit_scale=True,
                          unit_divisor=1024, colour=get_syslog_color()) as bar:

                    def progress(update: int, restart: bool):
                        if restart:
                            bar.reset()
                        else:
                            bar.update(update)
                    await self.__storage_client.write(entry.storage_uri, cast(BinaryIO, f),  progress=progress)
            else:
                _LOG.debug("Data already uploaded for entry %s", entry_id)

        self.__filled_entries.add(entry_id)

    async def copy_data(self, from_entry_id: str, to_uri: str) -> None:
        _LOG.debug("Attempt copying entry %s data to %s", from_entry_id, to_uri)
        entry = self.__entry_id_to_entry.get(from_entry_id, None)

        if entry is None:
            raise ValueError(f"Entry with id={from_entry_id} does not exist")

        if entry.storage_uri is None:
            raise ValueError(f"Entry with id={from_entry_id} has no storage uri")

        exists = await self.__storage_client.blob_exists(entry.storage_uri)
        if not exists:
            raise ValueError(f"Entry with id={from_entry_id} is not loaded to storage")

        await self.__storage_client.copy(entry.storage_uri, to_uri)

    def get(self, entry_id: str) -> SnapshotEntry:
        return self.__entry_id_to_entry[entry_id]
