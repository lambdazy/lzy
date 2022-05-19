import os
from typing import Any, Iterator

from lzy.v2.api import LzyWorkflow
from lzy.v2.api.env.default_env_provider import DefaultEnvProvider
from lzy.v2.api.env.env_provider import EnvProvider
from lzy.v2.api.lzy_dumper import LzyDumper
from lzy.v2.api.query import Query
from lzy.v2.api.runtime.runtime import Runtime
from lzy.v2.api.snapshot.snapshot_provider import SnapshotProvider
from lzy.v2.api.storage_spec import StorageSpec
from lzy.v2.in_mem.local_runtime import LocalRuntime
from lzy.v2.in_mem.local_snapshot_provider import LocalSnapshotProvider
from lzy.serialization.serializer import DefaultSerializer


class Lzy:
    def __init__(self, env_provider: EnvProvider = DefaultEnvProvider(),
                 runtime: Runtime = LocalRuntime(),
                 snapshot_provider: SnapshotProvider = LocalSnapshotProvider(),
                 storage_spec: StorageSpec = StorageSpec(),
                 lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy")):
        self._env_provider = env_provider
        self._runtime = runtime
        self._snapshot_provider = snapshot_provider
        self._storage_spec = storage_spec
        self._lzy_mount = lzy_mount
        self._serializer = DefaultSerializer()

    def add_dumper(self, dumper: LzyDumper) -> None:
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
