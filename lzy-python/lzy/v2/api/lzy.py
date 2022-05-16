import os
from typing import Any, Iterator

from lzy.v2.api import LzyWorkflow
from lzy.v2.api.env.default_env_provider import DefaultEnvProvider
from lzy.v2.api.env.env_provider import EnvProvider
from lzy.v2.api.query import Query
from lzy.v2.serialization.serializer import Serializer, DefaultSerializer


class Lzy:
    def __init__(self, env_provider: EnvProvider = DefaultEnvProvider(), serializer: Serializer = DefaultSerializer(),
                 lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy")):
        self._env_provider = env_provider
        self._serializer = serializer
        self._lzy_mount = lzy_mount

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def env_provider(self) -> EnvProvider:
        return self._env_provider

    @property
    def lzy_mount(self) -> str:
        return self._lzy_mount

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
        return LzyWorkflow(name, self.serializer, self.env_provider, self.lzy_mount, eager)
