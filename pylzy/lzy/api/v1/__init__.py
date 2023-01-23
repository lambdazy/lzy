import datetime
import inspect
import os
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, Iterable

from lzy.api.v1.call import LzyCall, wrap_call
from lzy.api.v1.env import DockerPullPolicy, Env
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.api.v1.provisioning import Provisioning, GpuType, CpuType
from lzy.api.v1.remote.runtime import RemoteRuntime, USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV
from lzy.api.v1.runtime import Runtime
from lzy.api.v1.snapshot import DefaultSnapshot
from lzy.api.v1.utils.conda import generate_conda_yaml
from lzy.api.v1.utils.packages import to_str_version
from lzy.api.v1.utils.proxy_adapter import lzy_proxy
from lzy.api.v1.utils.types import infer_return_type
from lzy.api.v1.whiteboards import whiteboard_
from lzy.api.v1.workflow import LzyWorkflow
from lzy.logs.config import configure_logging
from lzy.proxy.result import Nothing
from lzy.py_env.api import PyEnvProvider, PyEnv
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry, AsyncStorageClient
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.event_loop import LzyEventLoop
from lzy.whiteboards.api import WhiteboardManager, WhiteboardIndexClient
from lzy.whiteboards.index import WhiteboardIndexedManager, RemoteWhiteboardIndexClient, WB_USER_ENV, WB_KEY_PATH_ENV, \
    WB_ENDPOINT_ENV
# noinspection PyUnresolvedReferences
from lzy.whiteboards.wrapper import WhiteboardStatus, MISSING_WHITEBOARD_FIELD
from lzy.api.v1.utils.proxy_adapter import materialize

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
    provisioning: Provisioning = Provisioning(),
    cpu_type: Optional[str] = None,
    cpu_count: Optional[int] = None,
    gpu_type: Optional[str] = None,
    gpu_count: Optional[int] = None,
    ram_size_gb: Optional[int] = None,
    env: Env = Env(),
    description: str = "",
    lazy_arguments: bool = True
):
    def deco(f):
        """
        Decorator which will try to infer return type of function
        and create lazy constructor instead of decorated function.
        """

        nonlocal output_types
        if output_types is None:
            infer_result = infer_return_type(f)
            if isinstance(infer_result, Nothing):
                raise TypeError(
                    f"{f} return type is not annotated. "
                    f"Please for proper use of {op.__name__} "
                    f"annotate return type of your function."
                )
            else:
                output_types = infer_result.value  # expecting multiple return types

        nonlocal provisioning
        provisioning = provisioning.override(Provisioning(cpu_type, cpu_count, gpu_type, gpu_count, ram_size_gb))

        nonlocal libraries
        libraries = {} if not libraries else libraries

        nonlocal env
        env = env.override(
            Env(python_version, libraries, conda_yaml_path, docker_image, docker_pull_policy, local_modules_path)
        )

        # yep, create lazy constructor and return it
        # instead of function
        return wrap_call(f, output_types, provisioning, env, description, lazy_arguments)

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
        runtime: Runtime = RemoteRuntime(),
        whiteboard_client: WhiteboardIndexClient = RemoteWhiteboardIndexClient(),
        py_env_provider: PyEnvProvider = AutomaticPyEnvProvider(),
        storage_registry: StorageRegistry = DefaultStorageRegistry(),
        serializer_registry: LzySerializerRegistry = LzySerializerRegistry()
    ):
        configure_logging()

        self.__env_provider = py_env_provider
        self.__serializer_registry = serializer_registry
        self.__storage_registry = storage_registry
        self.__runtime = runtime
        self.__whiteboard_manager = WhiteboardIndexedManager(whiteboard_client, storage_registry, serializer_registry)

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
        provisioning: Provisioning = Provisioning.default(),
        interactive: bool = True,
        cpu_type: Optional[str] = None,
        cpu_count: Optional[int] = None,
        gpu_type: Optional[str] = None,
        gpu_count: Optional[int] = None,
        ram_size_gb: Optional[int] = None,
        env: Env = Env()
    ) -> LzyWorkflow:
        provisioning = provisioning.override(Provisioning(cpu_type, cpu_count, gpu_type, gpu_count, ram_size_gb))
        provisioning.validate()

        # it is important to detect py env before registering lazy calls to avoid materialization of them
        namespace = inspect.stack()[1].frame.f_globals
        auto_py_env: PyEnv = self.__env_provider.provide(namespace)

        libraries = {} if not libraries else libraries
        local_modules_path = auto_py_env.local_modules_path if not local_modules_path else local_modules_path
        env = env.override(
            Env(python_version, libraries, conda_yaml_path, docker_image, docker_pull_policy, local_modules_path)
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
                   id_: Optional[str],
                   storage_uri: Optional[str] = None,
                   storage_name: Optional[str] = None) -> Optional[Any]:
        return LzyEventLoop.run_async(
            self.whiteboard_manager.get(id_=id_, storage_uri=storage_uri, storage_name=storage_name))

    def whiteboards(self, *,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None) -> Iterable[Any]:
        it = self.whiteboard_manager.query(name=name, tags=tags, not_before=not_before, not_after=not_after)
        while True:
            try:
                # noinspection PyUnresolvedReferences
                elem = LzyEventLoop.run_async(it.__anext__())  # type: ignore
                yield elem
            except StopAsyncIteration:
                break
