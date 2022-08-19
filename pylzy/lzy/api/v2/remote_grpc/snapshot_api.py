from abc import ABC, abstractmethod
from typing import Optional

from ai.lzy.v1.oldwb.whiteboard_pb2 import Snapshot


class SnapshotApi(ABC):
    @abstractmethod
    def create(self, workflow_name: str) -> Snapshot:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str) -> None:
        pass

    @abstractmethod
    def last(self, workflow_name: str) -> Optional[Snapshot]:
        pass

    @abstractmethod
    def error(self, snapshot_id: str) -> None:
        pass
