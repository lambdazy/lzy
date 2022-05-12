from abc import ABC, abstractmethod

from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.serialization.serializer import Serializer


class SnapshotProvider(ABC):
    @abstractmethod
    def get(self, lzy_mount: str, serializer: Serializer) -> Snapshot:
        pass