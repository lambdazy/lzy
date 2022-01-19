from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Type, TypeVar
from dataclasses import dataclass
from enum import Enum
import uuid

from lzy.model.slot import Slot


@dataclass
class SnapshotDescription:
    snapshot_id: str


class WhiteboardFieldStatus(Enum):
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"


@dataclass
class WhiteboardFieldDescription:
    field_name: str
    storage_uri: Optional[str]
    dependent_field_names: Optional[List[str]]


class WhiteboardStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    NOT_COMPLETED = "NOT_COMPLETED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardInfo:
    id: str
    status: Optional[WhiteboardStatus]


@dataclass
class WhiteboardDescription:
    id: str
    fields: List[WhiteboardFieldDescription]
    snapshot: Optional[SnapshotDescription]
    status: Optional[WhiteboardStatus]


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
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        pass

    @abstractmethod
    def getAll(self) -> List[WhiteboardInfo]:
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

    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        return None

    def __init__(self) -> None:
        self.__whiteboards: Dict[str, WhiteboardDescription] = {}

    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        wb_id = str(uuid.uuid1())
        self.__whiteboards[wb_id] = WhiteboardDescription(
            wb_id,
            [WhiteboardFieldDescription(name, None, []) for name in fields],
            SnapshotDescription(snapshot_id=snapshot_id),
            WhiteboardStatus.CREATED
        )
        return self.__whiteboards[wb_id]
    
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    def get(self, wb_id: str) -> WhiteboardDescription:
        return self.__whiteboards[wb_id]

    def getAll(self) -> List[WhiteboardInfo]:
        return [WhiteboardInfo(wb.id, wb.status) for key, wb in self.__whiteboards.items()]


class InMemSnapshotApi(SnapshotApi):
    def create(self) -> SnapshotDescription:
        id = str(uuid.uuid1())
        return SnapshotDescription(id)
    
    def finalize(self, snapshot_id: str):
        pass
