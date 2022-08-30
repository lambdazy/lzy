import os
from typing import Any, Iterator, Optional

from lzy.api.v2.local.runtime import LocalRuntime
from lzy.api.v2.local.snapshot import LocalSnapshot
from lzy.api.v2.query import Query
from lzy.api.v2.runtime import Runtime
from lzy.api.v2.snapshot.snapshot import Snapshot
from lzy.api.v2.storage import StorageRegistry
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env_provider import EnvProvider
from lzy.env.lzy_env_provider import LzyEnvProvider
from lzy.serialization.api import SerializersRegistry
from lzy.serialization.registry import DefaultSerializersRegistry


class Lzy:
    def __init__(
        self,
        env_provider: EnvProvider = LzyEnvProvider(),
        runtime: Runtime = LocalRuntime(),
        storage_registry: StorageRegistry = StorageRegistry(),
    ):
        self._env_provider = env_provider
        self._runtime = runtime
        self._serializer = DefaultSerializersRegistry()
        self._storage_registry = storage_registry
        self._snapshot = LocalSnapshot()

    @property
    def serializer(self) -> SerializersRegistry:
        return self._serializer

    @property
    def env_provider(self) -> EnvProvider:
        return self._env_provider

    @property
    def runtime(self) -> Runtime:
        return self._runtime

    @property
    def storage_registry(self) -> StorageRegistry:
        return self._storage_registry

    @property
    def snapshot(self) -> Snapshot:
        return self._snapshot

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
        return LzyWorkflow(name, self, eager)
