import os
from typing import Any, Iterator

from lzy.v2.api import LzyWorkflow
from lzy.v2.api.env.default_env_provider import DefaultEnvProvider
from lzy.v2.api.env.env_provider import EnvProvider
from lzy.v2.api.query import Query
from lzy.v2.api.runtime.bash_runtime_provider import BashRuntimeProvider
from lzy.v2.api.runtime.runtime_provider import RuntimeProvider
from lzy.v2.api.snapshot.bash_snapshot_provider import BashSnapshotProvider
from lzy.v2.api.snapshot.snapshot_provider import SnapshotProvider
from lzy.v2.serialization.serializer import Serializer, DefaultSerializer


class Lzy:
    def __init__(self, env_provider: EnvProvider = DefaultEnvProvider(), serializer: Serializer = DefaultSerializer(),
                 snapshot_provider: SnapshotProvider = BashSnapshotProvider(),
                 runtime_provider: RuntimeProvider = BashRuntimeProvider(),
                 lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy")):
        self._env_provider = env_provider
        self._serializer = serializer
        self._lzy_mount = lzy_mount
        self._snapshot_provider = snapshot_provider
        self._runtime_provider = runtime_provider

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def env_provider(self) -> EnvProvider:
        return self.env_provider

    @property
    def lzy_mount(self) -> str:
        return self._lzy_mount

    @property
    def snapshot_provider(self):
        return self._snapshot_provider

    @property
    def runtime_provider(self):
        return self._runtime_provider

    def whiteboard(self, wid: str) -> Any:
        # TODO: implement
        pass

    def whiteboards(self, query: Query) -> Iterator[Any]:
        # TODO: implement
        pass

    # views(Iterator[Any], ViewType)
    # whiteboards(T).views(ViewType)
    # TODO: SQL for whiteboards?

    def workflow(
            self,
            name: str,
            eager: bool = False,
    ) -> LzyWorkflow:
        return LzyWorkflow(name, self, self._runtime_provider.get(self._lzy_mount, self._serializer),
                           self._snapshot_provider.get(self._lzy_mount, self._serializer), eager)
