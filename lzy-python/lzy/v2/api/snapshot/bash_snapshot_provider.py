import uuid

from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.api.snapshot.snapshot_impl import SnapshotImpl
from lzy.v2.api.snapshot.snapshot_provider import SnapshotProvider
from lzy.v2.serialization.serializer import Serializer
from lzy.v2.servant.bash_servant_client import BashServantClient
from lzy.v2.servant.v2.channel_manager import ServantChannelManager
from lzy.v2.servant.whiteboard_bash_api import SnapshotBashApi, WhiteboardBashApi


class BashSnapshotProvider(SnapshotProvider):
    def get(self, lzy_mount: str, serializer: Serializer) -> Snapshot:
        snapshot_id: str = str(uuid.uuid4())
        bash_servant_client = BashServantClient().instance(lzy_mount)
        bash_channel_manager = ServantChannelManager()
        return SnapshotImpl(snapshot_id, lzy_mount, SnapshotBashApi(lzy_mount),
                            WhiteboardBashApi(lzy_mount, bash_servant_client, serializer),
                            bash_channel_manager, serializer)
