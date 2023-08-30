import datetime
import inspect
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence, TypeVar, Iterable, ClassVar
from typing_extensions import Self

# This i'm considering as a part of API
from lzy.api.v1.runtime import Runtime
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.api.v1.remote.runtime import RemoteRuntime

from lzy.env.environment import LzyEnvironment
from lzy.env.container.docker import DockerContainer, DockerPullPolicy
from lzy.env.container.no_container import NoContainer
from lzy.env.provisioning.provisioning import Provisioning
from lzy.env.python.auto import AutoPythonEnv
from lzy.env.python.manual import ManualPythonEnv


# This just needed for this module and not part of API and will be removed aftewards
from lzy.core.call import LazyCallWrapper
from lzy.core.workflow import LzyWorkflow
from lzy.api.v1.remote.lzy_service_client import USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV
from lzy.api.v1.utils.types import infer_return_type
from lzy.api.v1.whiteboards import whiteboard_
from lzy.logs.config import configure_logging
from lzy.proxy.result import Absence
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry, AsyncStorageClient
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.format import pretty_function
from lzy.utils.event_loop import LzyEventLoop
from lzy.utils.cached_property import cached_property
from lzy.whiteboards.api import WhiteboardManager, WhiteboardIndexClient
from lzy.whiteboards.index import (
    WhiteboardIndexedManager,
    RemoteWhiteboardIndexClient,
    WB_USER_ENV,
    WB_KEY_PATH_ENV,
    WB_ENDPOINT_ENV,
)
from lzy.env.mixin import WithEnvironmentMixin


__all__ = [
    'Runtime', 'RemoteRuntime', 'LocalRuntime',
    'op', 'Lzy',
    'LzyEnvironment',
    'DockerContainer', 'DockerPullPolicy', 'NoContainer',
    'Provisioning',
    'AutoPythonEnv', 'ManualPythonEnv',
]


configure_logging()

FuncT = TypeVar(
    "FuncT",
    bound=Callable[..., Any],
)


def op(
    func: Optional[FuncT] = None,
    *,
    env: Optional[LzyEnvironment] = None,
    output_types: Optional[Sequence[type]] = None,
    description: str = "",
    version: str = "0.0",
    cache: bool = False,
    lazy_arguments: bool = False,
):
    def deco(f):
        """
        Decorator which will try to infer return type of function
        and create lazy constructor instead of decorated function.
        """

        nonlocal output_types
        if output_types is None:
            infer_result = infer_return_type(f)
            if isinstance(infer_result, Absence):
                raise TypeError(
                    f"Return type is not annotated for {pretty_function(f)}. "
                    f"Please for proper use of {op.__name__} "
                    f"annotate return type of your function."
                )

            output_types = infer_result.value  # expecting multiple return types

        # yep, create lazy constructor and return it
        # instead of function
        return LazyCallWrapper(
            function=f,
            output_types=output_types,
            env=env or LzyEnvironment(),
            description=description,
            version=version,
            cache=cache,
            lazy_arguments=lazy_arguments
        )

    if func is None:
        return deco

    return deco(func)


def whiteboard(name: str):
    def wrap(cls):
        return whiteboard_(cls, name)

    return wrap


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

        return LzyWorkflow(
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
