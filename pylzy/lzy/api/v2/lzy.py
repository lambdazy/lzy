import os
from typing import Any, Iterator

from lzy.api.v2.local.runtime import LocalRuntime
from lzy.api.v2.local.snapshot_provider import LocalSnapshotProvider
from lzy.api.v2.query import Query
from lzy.api.v2.runtime import Runtime
from lzy.api.v2.snapshot.snapshot_provider import SnapshotProvider
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env_provider import EnvProvider
from lzy.env.lzy_env_provider import LzyEnvProvider
from lzy.serialization.api import Dumper
from lzy.serialization.serializer import DefaultSerializer


class Lzy:
    def __init__(
        self,
        env_provider: EnvProvider = LzyEnvProvider(),
        runtime: Runtime = LocalRuntime(),
        snapshot_provider: SnapshotProvider = LocalSnapshotProvider(),
        lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy"),
    ):
        self._env_provider = env_provider
        self._runtime = runtime
        self._snapshot_provider = snapshot_provider
        self._lzy_mount = lzy_mount
        self._serializer = DefaultSerializer()

    def add_dumper(self, dumper: Dumper) -> None:
        self._serializer.add_dumper(dumper)

    @property
    def env_provider(self) -> EnvProvider:
        return self._env_provider

    @property
    def lzy_mount(self) -> str:
        return self._lzy_mount

    @property
    def runtime(self) -> Runtime:
        return self._runtime

    @property
    def snapshot_provider(self) -> SnapshotProvider:
        return self._snapshot_provider

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
        return LzyWorkflow(name, self.lzy_mount, self, eager)
