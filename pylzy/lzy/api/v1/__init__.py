import datetime
import inspect
import os
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, Iterable, Mapping

from lzy.api.v1.call import LzyCall, wrap_call
from lzy.api.v1.env import DockerPullPolicy, Env, DockerCredentials
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.api.v1.provisioning import (
    Provisioning,
    GpuType,
    CpuType,
    IntegerRequirement,
    StringRequirement
)
from lzy.api.v1.remote.runtime import RemoteRuntime
from lzy.api.v1.remote.workflow_service_client import USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV, WorkflowServiceClient
from lzy.api.v1.runtime import Runtime
from lzy.api.v1.snapshot import DefaultSnapshot
from lzy.api.v1.utils.conda import generate_conda_yaml
from lzy.api.v1.utils.packages import to_str_version
from lzy.api.v1.utils.proxy_adapter import lzy_proxy, materialize
from lzy.api.v1.utils.types import infer_return_type
from lzy.api.v1.whiteboards import whiteboard_
from lzy.api.v1.workflow import LzyWorkflow
from lzy.logs.config import configure_logging
from lzy.proxy.result import Absence
from lzy.py_env.api import PyEnvProvider, PyEnv
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry, AsyncStorageClient
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.format import pretty_function
from lzy.utils.event_loop import LzyEventLoop
from lzy.whiteboards.api import WhiteboardManager, WhiteboardIndexClient
from lzy.whiteboards.index import WhiteboardIndexedManager, RemoteWhiteboardIndexClient, WB_USER_ENV, WB_KEY_PATH_ENV, \
    WB_ENDPOINT_ENV
# noinspection PyUnresolvedReferences
from lzy.whiteboards.wrapper import WhiteboardStatus, MISSING_WHITEBOARD_FIELD

configure_logging()
T = TypeVar("T")  # pylint: disable=invalid-name

FuncT = TypeVar(
    "FuncT",
    bound=Callable[..., Any],
)


# pylint: disable=[invalid-name]
# noinspection PyShadowingNames
def op(
    func: Optional[FuncT] = None,
    *,
    output_types: Optional[Sequence[type]] = None,
    python_version: Optional[str] = None,
    libraries: Optional[Dict[str, str]] = None,
    conda_yaml_path: Optional[str] = None,
    docker_image: Optional[str] = None,
    docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
    local_modules_path: Optional[Sequence[str]] = None,
    provisioning: Optional[Provisioning] = None,
    cpu_type: StringRequirement = None,
    cpu_count: IntegerRequirement = None,
    gpu_type: StringRequirement = None,
    gpu_count: IntegerRequirement = None,
    ram_size_gb: IntegerRequirement = None,
    env: Env = Env(),
    description: str = "",
    version: str = "0.0",
    cache: bool = False,
    lazy_arguments: bool = False,
    env_variables: Optional[Mapping[str, str]] = None,
    docker_credentials: Optional[DockerCredentials] = None
):
    libraries = libraries or {}
    env_variables = env_variables or {}
    provisioning = provisioning or Provisioning()
    provisioning = provisioning.override(
        cpu_type=cpu_type,
        cpu_count=cpu_count,
        gpu_type=gpu_type,
        gpu_count=gpu_count,
        ram_size_gb=ram_size_gb
    )

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
            else:
                output_types = infer_result.value  # expecting multiple return types

        nonlocal provisioning
        nonlocal libraries
        nonlocal env
        env = env.override(
            Env(python_version, libraries, conda_yaml_path, docker_image, docker_pull_policy, local_modules_path,
                env_variables=env_variables, docker_credentials=docker_credentials)
        )

        # yep, create lazy constructor and return it
        # instead of function
        return wrap_call(f, output_types, provisioning, env, description, version, cache, lazy_arguments)

    if func is None:
        return deco

    return deco(func)


def whiteboard(name: str):
    def wrap(cls):
        return whiteboard_(cls, name)

    return wrap


def lzy_auth(*, user: str, key_path: str, endpoint: Optional[str] = None,
             whiteboards_endpoint: Optional[str] = None) -> None:
    os.environ[USER_ENV] = user
    os.environ[WB_USER_ENV] = user
    os.environ[KEY_PATH_ENV] = key_path
    os.environ[WB_KEY_PATH_ENV] = key_path
    if endpoint is not None:
        os.environ[ENDPOINT_ENV] = endpoint
    if whiteboards_endpoint is not None:
        os.environ[WB_ENDPOINT_ENV] = whiteboards_endpoint


class Lzy:
    # noinspection PyShadowingNames
    def __init__(
        self,
        *,
        runtime: Optional[Runtime] = None,
        whiteboard_client: Optional[WhiteboardIndexClient] = None,
        py_env_provider: Optional[PyEnvProvider] = None,
        storage_registry: Optional[StorageRegistry] = None,
        serializer_registry: Optional[LzySerializerRegistry] = None
    ):
        whiteboard_index_client = RemoteWhiteboardIndexClient() if whiteboard_client is None else whiteboard_client
        self.__runtime = RemoteRuntime() if runtime is None else runtime
        self.__storage_registry = DefaultStorageRegistry() if storage_registry is None else storage_registry
        self.__registered_runtime_storage: bool = False

        self.__env_provider = AutomaticPyEnvProvider() if py_env_provider is None else py_env_provider
        self.__serializer_registry = LzySerializerRegistry() if serializer_registry is None else serializer_registry
        self.__whiteboard_manager = WhiteboardIndexedManager(whiteboard_index_client, self.__storage_registry,
                                                             self.__serializer_registry)

        self.__storage_client: Optional[AsyncStorageClient] = None
        self.__storage_name: Optional[str] = None
        self.__storage_uri: Optional[str] = None

    @staticmethod
    def auth(*, user: str, key_path: str, endpoint: Optional[str] = None,
             whiteboards_endpoint: Optional[str] = None) -> None:
        lzy_auth(user=user, key_path=key_path, endpoint=endpoint, whiteboards_endpoint=whiteboards_endpoint)

    @property
    def serializer_registry(self) -> LzySerializerRegistry:
        return self.__serializer_registry

    @property
    def env_provider(self) -> PyEnvProvider:
        return self.__env_provider

    @property
    def runtime(self) -> Runtime:
        return self.__runtime

    @property
    def storage_registry(self) -> StorageRegistry:
        return self.__storage_registry

    @property
    def whiteboard_manager(self) -> WhiteboardManager:
        return self.__whiteboard_manager

    @property
    def storage_name(self) -> str:
        if self.__storage_name is None:
            name = self.storage_registry.default_storage_name()
            if name is None:
                raise ValueError(
                    f"Cannot get storage name, default storage config is not set"
                )
            self.__storage_name = name
            return name
        return self.__storage_name

    @property
    def storage_uri(self) -> str:
        if self.__storage_uri is None:
            conf = self.storage_registry.default_config()
            if conf is None:
                raise ValueError(
                    f"Cannot get storage bucket, default storage config is not set"
                )
            self.__storage_uri = conf.uri
            return conf.uri
        return self.__storage_uri

    @property
    def storage_client(self) -> AsyncStorageClient:
        if self.__storage_client is None:
            client = self.__storage_registry.default_client()
            if client is None:
                raise ValueError(
                    f"Cannot get storage client, default storage config is not set"
                )
            self.__storage_client = client
            return client
        return self.__storage_client

    # noinspection PyShadowingNames
    def workflow(
        self,
        name: str,
        *,
        eager: bool = False,
        python_version: Optional[str] = None,
        libraries: Optional[Dict[str, str]] = None,
        conda_yaml_path: Optional[str] = None,
        docker_image: Optional[str] = None,
        docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
        local_modules_path: Optional[Sequence[str]] = None,
        interactive: bool = True,
        provisioning: Optional[Provisioning] = None,
        cpu_type: StringRequirement = None,
        cpu_count: IntegerRequirement = None,
        gpu_type: StringRequirement = None,
        gpu_count: IntegerRequirement = None,
        ram_size_gb: IntegerRequirement = None,
        env_variables: Optional[Mapping[str, str]] = None,
        docker_credentials: Optional[DockerCredentials] = None,
        env: Env = Env(),
        exclude_packages: Iterable[str] = tuple()
    ) -> LzyWorkflow:
        self.__register_default_runtime_storage()

        env_variables = env_variables or {}
        libraries = libraries or {}

        provisioning = provisioning or Provisioning()
        provisioning = provisioning.override(
            cpu_type=cpu_type,
            cpu_count=cpu_count,
            gpu_type=gpu_type,
            gpu_count=gpu_count,
            ram_size_gb=ram_size_gb
        )

        # it is important to detect py env before registering lazy calls to avoid materialization of them
        frame = inspect.stack()[1].frame
        namespace = {**frame.f_globals, **frame.f_locals}

        auto_py_env = self.__env_provider.provide(namespace, exclude_packages)
        local_modules_path = auto_py_env.local_modules_path if not local_modules_path else local_modules_path

        env = env.override(
            Env(python_version, libraries, conda_yaml_path, docker_image, docker_pull_policy, local_modules_path,
                env_variables=env_variables, docker_credentials=docker_credentials)
        )
        env.validate()

        return LzyWorkflow(
            name,
            self,
            env=env,
            provisioning=provisioning,
            auto_py_env=auto_py_env,
            eager=eager,
            interactive=interactive
        )

    def whiteboard(self, *,
                   id_: Optional[str] = None,
                   storage_uri: Optional[str] = None,
                   storage_name: Optional[str] = None) -> Optional[Any]:
        self.__register_default_runtime_storage()
        return LzyEventLoop.run_async(
            self.whiteboard_manager.get(id_=id_, storage_uri=storage_uri, storage_name=storage_name))

    def whiteboards(self, *,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None) -> Iterable[Any]:
        self.__register_default_runtime_storage()
        it = self.whiteboard_manager.query(name=name, tags=tags, not_before=not_before, not_after=not_after)
        while True:
            try:
                # noinspection PyUnresolvedReferences
                elem = LzyEventLoop.run_async(it.__anext__())  # type: ignore
                yield elem
            except StopAsyncIteration:
                break

    def __register_default_runtime_storage(self) -> None:
        if self.__registered_runtime_storage:
            return

        default_storage = LzyEventLoop.run_async(self.__runtime.storage())
        if default_storage:
            as_default = self.__storage_registry.default_client() is None
            self.__storage_registry.register_storage("provided_default_storage", default_storage, default=as_default)
        self.__registered_runtime_storage = True
