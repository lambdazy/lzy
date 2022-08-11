from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from lzy.api.v2.snapshot.snapshot import Snapshot

if TYPE_CHECKING:
    from lzy.serialization.api import Serializer


class SnapshotProvider(ABC):
    @abstractmethod
    def get(self, lzy_mount: str, serializer: "Serializer") -> Snapshot:
        pass
