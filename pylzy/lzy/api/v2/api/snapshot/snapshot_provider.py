from abc import ABC, abstractmethod

from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.serialization.serializer import Serializer


class SnapshotProvider(ABC):
    @abstractmethod
    def get(self, lzy_mount: str, serializer: Serializer) -> Snapshot:
        pass
