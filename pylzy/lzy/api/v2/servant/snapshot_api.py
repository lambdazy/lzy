from abc import ABC, abstractmethod
from typing import Optional

from lzy.proto.bet.priv.v2.__init__ import Snapshot


class SnapshotApi(ABC):
    @abstractmethod
    def create(self, workflow_name: str) -> Snapshot:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str):
        pass

    @abstractmethod
    def last(self, workflow_name: str) -> Optional[Snapshot]:
        pass

    @abstractmethod
    def error(self, snapshot_id: str):
        pass
