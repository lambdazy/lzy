from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Type
from dataclasses import dataclass
from enum import Enum
import uuid

from lzy.model.slot import Slot


@dataclass
class SnapshotDescription:
    snapshot_id: str


class WhiteboardFieldStatus(Enum):
    IN_PROGRESS = 0
    FINISHED = 1


@dataclass
class WhiteboardFieldDescription:
    field_name: str
    storage_uri: Optional[str]
    dependent_field_names: Optional[List[str]]
    is_empty: bool
    status: Optional[WhiteboardFieldStatus]


class WhiteboardStatus(Enum):
    UNKNOWN = 0
    CREATED = 1
    COMPLETED = 2
    NOT_COMPLETED = 3
    ERRORED = 4


@dataclass
class WhiteboardDescription:
    id: str
    fields: List[WhiteboardFieldDescription]
    snapshot: SnapshotDescription
    status: WhiteboardStatus


class SnapshotApi(ABC):
    @abstractmethod
    def create(self) -> SnapshotDescription:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str):
        pass


class WhiteboardApi(ABC):

    @abstractmethod
    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        pass

    @abstractmethod
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    @abstractmethod
    def get(self, wb_id: str) -> WhiteboardDescription:
        pass

    @abstractmethod
    def resolve(self, wb_id: str, field_name: str, typ: Type[Any]) -> Any:
        pass


class EntryIdGenerator(ABC):
    
    @abstractmethod
    def generate(self, slot: Slot) -> str:
        pass


class UUIDEntryIdGenerator(EntryIdGenerator):
    def __init__(self, snapshot_id: str) -> None:
        self.__snapshot_id = snapshot_id
    
    def generate(self, slot: Slot) -> str:
        return "/".join([self.__snapshot_id, slot.name, str(uuid.uuid1())])


class InMemWhiteboardApi(WhiteboardApi):

    def resolve(self, wb_id: str, field_name: str, typ: Type[Any]) -> Any:
        return None

    def __init__(self) -> None:
        self.__whiteboards: Dict[str, WhiteboardDescription] = {}

    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        wb_id = str(uuid.uuid1())
        self.__whiteboards[wb_id] = WhiteboardDescription(
            wb_id,
            [WhiteboardFieldDescription(name, None, [], True, WhiteboardFieldStatus.IN_PROGRESS) for name in fields],
            SnapshotDescription(snapshot_id=snapshot_id),
            WhiteboardStatus.CREATED
        )
        return self.__whiteboards[wb_id]
    
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    def get(self, wb_id: str) -> WhiteboardDescription:
        return self.__whiteboards[wb_id]


class InMemSnapshotApi(SnapshotApi):
    def create(self) -> SnapshotDescription:
        id = str(uuid.uuid1())
        return SnapshotDescription(id)
    
    def finalize(self, snapshot_id: str):
        pass
