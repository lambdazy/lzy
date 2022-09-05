from typing import Any, Iterator

from lzy.api.v2.local.runtime import LocalRuntime
from lzy.api.v2.query import Query
from lzy.api.v2.runtime import Runtime
from lzy.api.v2.snapshot import DefaultSnapshot
from lzy.api.v2.whiteboard_declaration import whiteboard_
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env_provider import EnvProvider
from lzy.env.lzy_env_provider import LzyEnvProvider
from lzy.serialization.api import SerializerRegistry
from lzy.serialization.registry import DefaultSerializerRegistry
from lzy.storage.api import StorageRegistry
from lzy.storage.registry import DefaultStorageRegistry


def whiteboard(name: str):
    def wrap(cls):
        return whiteboard_(cls, "default", name)

    return wrap


class Lzy:
    def __init__(
        self,
        runtime: Runtime = LocalRuntime(),
        env_provider: EnvProvider = LzyEnvProvider(),
        storage_registry: StorageRegistry = DefaultStorageRegistry(),
        serializer_registry: SerializerRegistry = DefaultSerializerRegistry(),
    ):
        self.__env_provider = env_provider
        self.__runtime = runtime
        self.__serializer_registry = serializer_registry
        self.__storage_registry = storage_registry

    @property
    def serializer(self) -> SerializerRegistry:
        return self.__serializer_registry

    @property
    def env_provider(self) -> EnvProvider:
        return self.__env_provider

    @property
    def runtime(self) -> Runtime:
        return self.__runtime

    @property
    def storage_registry(self) -> StorageRegistry:
        return self.__storage_registry

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
        return LzyWorkflow(
            name,
            self,
            DefaultSnapshot(self.__storage_registry, self.__serializer_registry),
            eager,
        )
