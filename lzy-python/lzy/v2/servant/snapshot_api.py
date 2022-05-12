from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class SnapshotDescription:
    snapshot_id: str


class SnapshotApi(ABC):
    @abstractmethod
    def create(self, workflow_name: str) -> SnapshotDescription:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str):
        pass

    @abstractmethod
    def last(self, workflow_name: str) -> Optional[SnapshotDescription]:
        pass

    @abstractmethod
    def error(self, snapshot_id: str):
        pass
