from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Type, Any

from lzy.api.v2.servant.snapshot_api import SnapshotDescription


class WhiteboardFieldStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardFieldDescription:
    field_name: str
    status: WhiteboardFieldStatus
    dependent_field_names: Optional[List[str]]  # protobuf makes no distinction between empty list and null list
    storage_uri: str


class WhiteboardStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardDescription:
    id: str  # pylint: disable=invalid-name
    fields: List[WhiteboardFieldDescription]
    snapshot: SnapshotDescription
    status: WhiteboardStatus


class WhiteboardApi(ABC):
    @abstractmethod
    def create(self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]) -> WhiteboardDescription:
        pass

    @abstractmethod
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    @abstractmethod
    def get(self, wb_id: str) -> WhiteboardDescription:
        pass

    @abstractmethod
    def list(self, namespace: str, tags: List[str], from_date: datetime = None, to_date: datetime = None) -> \
            List[WhiteboardDescription]:
        pass

    @abstractmethod
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        pass
