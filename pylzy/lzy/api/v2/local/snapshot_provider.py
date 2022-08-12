from lzy.api.v2.local.snapshot import LocalSnapshot
from lzy.api.v2.snapshot.snapshot import Snapshot
from lzy.api.v2.snapshot.snapshot_provider import SnapshotProvider
from lzy.serialization.api import SerializersRegistry


class LocalSnapshotProvider(SnapshotProvider):
    def get(self, lzy_mount: str, serializer: "SerializersRegistry") -> Snapshot:
        return LocalSnapshot()
