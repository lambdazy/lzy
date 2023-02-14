import asyncio
import dataclasses
import hashlib
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from io import FileIO
from typing import Any, Dict, Type, cast, BinaryIO, Set, Union, List

from serialzy.api import Schema, SerializerRegistry
from tqdm import tqdm

from lzy.api.v1.utils.proxy_adapter import type_is_lzy_proxy
from lzy.logs.config import get_logger, get_color
from lzy.proxy.result import Just, Nothing, Result
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
    storage_uri: Union[str, None] = None
    hash: Union[str, None] = None

    def set_storage_uri(self, uri: str) -> None:
        self.storage_uri = uri

    def set_hash(self, hsh: str) -> None:
        self.hash = hsh


class Snapshot(ABC):  # pragma: no cover
    @abstractmethod
    def create_entry(self, name: str, typ: Type) -> SnapshotEntry:
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

    @abstractmethod
    def must_be_copied(self, entry_id: str, wb_uri: str) -> None:
        pass

    @abstractmethod
    async def processed_copying(self, copy_func) -> None:
        pass


class SerializedDataHasher:
    @staticmethod
    def hash_of_str(uri: str) -> str:
        return hashlib.sha256(uri.encode('utf-8')).hexdigest()

    @staticmethod
    def hash_of_file(file_obj: FileIO) -> str:
        blocksize: int = 4096
        hsh = hashlib.sha256()
        while True:
            buf = file_obj.read(blocksize)
            if not buf:
                break
            hsh.update(buf)
        return hsh.hexdigest()


class DefaultSnapshot(Snapshot):
    def __init__(
            self,
            workflow_name: str,
            serializer_registry: SerializerRegistry,
            storage_uri: str,
            storage_client: AsyncStorageClient,
            storage_name: str
    ):
        self.__serializer_registry = serializer_registry
        self.__storage_client = storage_client
        self.__storage_name = storage_name
        self.__input_prefix = f"{storage_uri}/lzy_runs/${workflow_name}/inputs"
        self.__op_result_prefix = f"${storage_uri}/lzy_runs/${workflow_name}/ops"
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

        schema = serializer_by_type.schema(typ)
        e = SnapshotEntry(eid, name, typ, schema, self.__storage_name)
        self.__entry_id_to_entry[e.id] = e
        _LOG.debug(f"Created entry {e}")
        return e

    def update_entry(self, entry_id: str, storage_uri: str) -> None:
        if entry_id in self.__filled_entries:
            raise ValueError(f"Cannot update entry {entry_id}: data has been already uploaded")
        entry = self.get(entry_id)
        if type_is_lzy_proxy(entry.typ):
            entry.set_storage_uri(self.__op_result_prefix + storage_uri)
        else:
            entry.set_storage_uri(self.__input_prefix + storage_uri)

    async def get_data(self, entry_id: str) -> Result[Any]:
        _LOG.debug(f"Getting data for entry {entry_id}")
        entry = self.__entry_id_to_entry.get(entry_id, None)
        if entry is None:
            raise ValueError(f"Entry with id={entry_id} does not exist")

        if entry.storage_uri is None:
            return Nothing()

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

            data_hash: str = SerializedDataHasher.hash_of_file(f)
            f.seek(0)

            self.__entry_id_to_entry[entry_id].set_hash(data_hash)
            self.update_entry(entry_id, f"/{data_hash}")

            exists = await self.__storage_client.blob_exists(entry.storage_uri)
            if not exists:
                with tqdm(total=length, desc=f"Uploading {entry.name}", file=sys.stdout, unit='B', unit_scale=True,
                          unit_divisor=1024, colour=get_color()) as bar:
                    await self.__storage_client.write(entry.storage_uri, cast(BinaryIO, f),
                                                      progress=lambda x: bar.update(x))

        self.__filled_entries.add(entry_id)

    def get(self, entry_id: str) -> SnapshotEntry:
        return self.__entry_id_to_entry[entry_id]

    def must_be_copied(self, entry_id: str, wb_uri: str) -> None:
        self.__copy_queue.setdefault(entry_id, []).append(wb_uri)

    async def processed_copying(self, copy_func) -> None:
        data_to_load = []
        for src, dest in self.__copy_queue.items():
            for d in dest:
                data_to_load.append(copy_func(self.__entry_id_to_entry[src].storage_uri, d))

        await asyncio.gather(*data_to_load)
