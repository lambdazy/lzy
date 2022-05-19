from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.api.snapshot.snapshot_provider import SnapshotProvider
from lzy.v2.in_mem.local_snapshot import LocalSnapshot
from lzy.serialization.serializer import Serializer


class LocalSnapshotProvider(SnapshotProvider):
    def get(self, lzy_mount: str, serializer: Serializer) -> Snapshot:
        return LocalSnapshot()
