from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional, Type

from ai.lzy.v1.oldwb.whiteboard_old_pb2 import Whiteboard

# class WhiteboardFieldStatus(Enum):
#     UNKNOWN = "UNKNOWN"
#     CREATED = "CREATED"
#     IN_PROGRESS = "IN_PROGRESS"
#     FINISHED = "FINISHED"
#     ERRORED = "ERRORED"


# @dataclass
# class WhiteboardFieldDescription:
#     field_name: str
#     status: WhiteboardFieldStatus
#     dependent_field_names: Optional[
#         List[str]
#     ]  # protobuf makes no distinction between empty list and null list
#     storage_uri: str


# class WhiteboardStatus(Enum):
#     UNKNOWN = "UNKNOWN"
#     CREATED = "CREATED"
#     COMPLETED = "COMPLETED"
#     ERRORED = "ERRORED"


# @dataclass
# class WhiteboardDescription:
#     id: str  # pylint: disable=invalid-name
#     fields: List[WhiteboardFieldDescription]
#     snapshot: SnapshotDescription
#     status: WhiteboardStatus


class WhiteboardApi(ABC):
    @abstractmethod
    def create(
        self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]
    ) -> Whiteboard:
        pass

    @abstractmethod
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    @abstractmethod
    def get(self, wb_id: str) -> Whiteboard:
        pass

    @abstractmethod
    def list(
        self,
        namespace: str,
        tags: List[str],
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> List[Whiteboard]:
        pass

    @abstractmethod
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        pass
