from __future__ import annotations

import os
import inspect
import datetime
from dataclasses import dataclass, field
from functools import cached_property

from typing import Any, Optional, Sequence, Iterable, ClassVar, Type
from typing_extensions import Self

from lzy.api.v1.runtime import Runtime
from lzy.api.v1.remote.lzy_service_client import USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV
from lzy.api.v1.remote.runtime import RemoteRuntime
from lzy.core.workflow import LzyWorkflow
from lzy.env.environment import LzyEnvironment
from lzy.env.mixin import WithEnvironmentMixin
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry, AsyncStorageClient
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.event_loop import LzyEventLoop
from lzy.whiteboards.api import WhiteboardIndexClient
from lzy.whiteboards.index import WB_USER_ENV, WB_KEY_PATH_ENV, WB_ENDPOINT_ENV
from lzy.whiteboards.index import WhiteboardIndexedManager, RemoteWhiteboardIndexClient


def lzy_auth(
    *,
    user: str,
    key_path: str,
    endpoint: Optional[str] = None,
    whiteboards_endpoint: Optional[str] = None
) -> None:

    os.environ[USER_ENV] = user
    os.environ[WB_USER_ENV] = user
    os.environ[KEY_PATH_ENV] = key_path
    os.environ[WB_KEY_PATH_ENV] = key_path
    if endpoint is not None:
        os.environ[ENDPOINT_ENV] = endpoint
    if whiteboards_endpoint is not None:
        os.environ[WB_ENDPOINT_ENV] = whiteboards_endpoint


@dataclass(frozen=True)
class Lzy(WithEnvironmentMixin):
    env: LzyEnvironment = field(default_factory=LzyEnvironment)
    runtime: Runtime = field(default_factory=RemoteRuntime)
    whiteboard_client: WhiteboardIndexClient = field(default_factory=RemoteWhiteboardIndexClient)
    storage_registry: StorageRegistry = field(default_factory=DefaultStorageRegistry)
    serializer_registry: LzySerializerRegistry = field(default_factory=LzySerializerRegistry)

    _registered_runtime_storage: bool = field(init=False)

    _workflow_class: ClassVar[Type[LzyWorkflow]] = LzyWorkflow

    def __post_init__(self):
        object.__setattr__(self, '_registered_runtime_storage', False)

    def with_fields(self, **kwargs: Any) -> Self:
        if self._registered_runtime_storage:
            raise RuntimeError(
                "It is fobidden to change Lzy fields after working "
                "with whiteboards"
            )

        return super().with_fields(**kwargs)

    def auth(
        self, *,
        user: str,
        key_path: str,
        endpoint: Optional[str] = None,
        whiteboards_endpoint: Optional[str] = None
    ) -> Self:
        lzy_auth(
            user=user,
            key_path=key_path,
            endpoint=endpoint,
            whiteboards_endpoint=whiteboards_endpoint
        )
        return self

    @cached_property
    def whiteboard_manager(self) -> WhiteboardIndexedManager:
        return WhiteboardIndexedManager(
            self.whiteboard_client,
            self.storage_registry,
            self.serializer_registry
        )

    @cached_property
    def storage_name(self) -> str:
        name = self.storage_registry.default_storage_name()
        if name is None:
            raise ValueError("Cannot get storage name, default storage config is not set")
        return name

    @cached_property
    def storage_uri(self) -> str:
        conf = self.storage_registry.default_config()
        if conf is None:
            raise ValueError("Cannot get storage bucket, default storage config is not set")
        return conf.uri

    @cached_property
    def storage_client(self) -> AsyncStorageClient:
        client = self.storage_registry.default_client()
        if client is None:
            raise ValueError("Cannot get storage client, default storage config is not set")
        return client

    def workflow(
        self,
        name: str,
        *,
        eager: bool = False,
        interactive: bool = True,
        env: Optional[LzyEnvironment] = None,
    ) -> LzyWorkflow:
        self._register_default_runtime_storage()

        frame = inspect.stack()[1].frame
        namespace = {**frame.f_globals, **frame.f_locals}

        env = env or LzyEnvironment()
        env = env.with_fields(
            namespace={**namespace, **env.get_namespace()}
        )

        return self._workflow_class(
            name=name,
            owner=self,
            env=env,
            eager=eager,
            interactive=interactive
        )

    def whiteboard(
        self, *,
        id_: Optional[str] = None,
        storage_uri: Optional[str] = None,
        storage_name: Optional[str] = None
    ) -> Optional[Any]:
        self._register_default_runtime_storage()
        return LzyEventLoop.run_async(
            self.whiteboard_manager.get(id_=id_, storage_uri=storage_uri, storage_name=storage_name)
        )

    def whiteboards(
        self, *,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> Iterable[Any]:
        self._register_default_runtime_storage()
        it = self.whiteboard_manager.query(name=name, tags=tags, not_before=not_before, not_after=not_after)
        while True:
            try:
                # noinspection PyUnresolvedReferences
                elem = LzyEventLoop.run_async(it.__anext__())  # type: ignore
                yield elem
            except StopAsyncIteration:
                break

    def _register_default_runtime_storage(self) -> None:
        if self._registered_runtime_storage:
            return

        default_storage = LzyEventLoop.run_async(self.runtime.storage())
        if default_storage:
            as_default = self.storage_registry.default_client() is None
            self.storage_registry.register_storage("provided_default_storage", default_storage, default=as_default)

        object.__setattr__(self, '_registered_runtime_storage', True)
