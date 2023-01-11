import dataclasses
import logging
import tempfile
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type, cast, BinaryIO

from serialzy.api import SerializerRegistry, Schema

from lzy.proxy.result import Just, Nothing, Result
from lzy.storage.api import AsyncStorageClient, StorageRegistry

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class DataScheme:
    type: str
    scheme_type: str


@dataclasses.dataclass(frozen=True)
class SnapshotEntry:
    id: str
    typ: Type
    storage_url: str
    storage_name: Optional[str]
    data_scheme: Schema


class Snapshot(ABC):
    @abstractmethod
    def create_entry(self, typ: Type) -> SnapshotEntry:
        # TODO (tomato): add prefixes to entry
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

    @abstractmethod
    def storage_name(self) -> str:
        pass


class DefaultSnapshot(Snapshot):
    def __init__(
            self, storage_registry: StorageRegistry, serializer_registry: SerializerRegistry
    ):
        self.__storage_registry = storage_registry
        self.__serializer_registry = serializer_registry

        self.__storage_client: Optional[AsyncStorageClient] = None
        self.__storage_name: Optional[str] = None
        self.__entry_id_to_entry: Dict[str, SnapshotEntry] = {}
        self.__storage_bucket: Optional[str] = None

    def create_entry(self, typ: Type) -> SnapshotEntry:
        eid = str(uuid.uuid4())
        url = self.storage_client.generate_uri(self.storage_bucket, eid)
        serializer_by_type = self.__serializer_registry.find_serializer_by_type(typ)
        if serializer_by_type is None:
            raise ValueError(f'Cannot find serializer for type {typ}')
        if not serializer_by_type.available():
            raise ValueError(
                f'Serializer for type {typ} is not available, please install {serializer_by_type.requirements()}')

        schema = serializer_by_type.schema(typ)
        e = SnapshotEntry(eid, typ, url, self.storage_name(), data_scheme=schema)
        self.__entry_id_to_entry[e.id] = e
        _LOG.debug(f"Created entry {e}")
        return e

    async def get_data(self, entry_id: str) -> Result[Any]:
        _LOG.debug(f"Getting data for entry {entry_id}")
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            return Nothing()

        exists = await self.storage_client.blob_exists(entry.storage_url)
        if not exists:
            return Nothing()

        with tempfile.NamedTemporaryFile() as f:
            await self.storage_client.read(entry.storage_url, cast(BinaryIO, f))
            f.seek(0)
            res = self.__serializer_registry.find_serializer_by_type(entry.typ).deserialize(f)
            return Just(res)

    async def put_data(self, entry_id: str, data: Any) -> None:
        _LOG.debug(f"Putting data for entry {entry_id}")
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Cannot get entry {entry_id}")

        with tempfile.NamedTemporaryFile() as f:
            self.__serializer_registry.find_serializer_by_type(entry.typ).serialize(data, f)
            f.seek(0)
            await self.storage_client.write(entry.storage_url, cast(BinaryIO, f))

    def get(self, entry_id: str) -> SnapshotEntry:
        return self.__entry_id_to_entry[entry_id]

    def storage_name(self) -> str:
        if self.__storage_name is None:
            name = self.__storage_registry.default_storage_name()
            if name is None:
                raise ValueError(
                    f"Cannot get storage name, default storage config is not set"
                )
            self.__storage_name = name
            return name
        return self.__storage_name

    @property
    def storage_bucket(self) -> str:
        if self.__storage_bucket is None:
            conf = self.__storage_registry.default_config()
            if conf is None:
                raise ValueError(
                    f"Cannot get storage bucket, default storage config is not set"
                )
            self.__storage_bucket = conf.bucket
            return conf.bucket
        return self.__storage_bucket

    @property
    def storage_client(self) -> AsyncStorageClient:
        if self.__storage_client is None:
            client = self.__storage_registry.default_client()
            if client is None:
                raise ValueError(
                    f"Cannot get storage client, default storage config is not set"
                )
            self.__storage_client = client
            return client
        return self.__storage_client
