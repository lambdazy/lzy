import datetime
import inspect
import os
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, Iterable, Mapping

from pypi_simple import PYPI_SIMPLE_ENDPOINT

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
from lzy.utils.pip import Pip
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
    env: Optional[Env] = None,
    description: str = "",
    version: str = "0.0",
    cache: bool = False,
    lazy_arguments: bool = False,
    env_variables: Optional[Mapping[str, str]] = None,
    docker_credentials: Optional[DockerCredentials] = None
):
    provisioning = provisioning or Provisioning()
    provisioning = provisioning.override(
        cpu_type=cpu_type,
        cpu_count=cpu_count,
        gpu_type=gpu_type,
        gpu_count=gpu_count,
        ram_size_gb=ram_size_gb
    )

    env = env or Env()
    env = env.override(
        env_variables=env_variables,
        python_version=python_version,
        libraries=libraries,
        local_modules_path=local_modules_path,
        conda_yaml_path=conda_yaml_path,
        docker_image=docker_image,
        docker_pull_policy=docker_pull_policy,
        docker_credentials=docker_credentials,
    )

    assert not env.pypi_index_url, \
        'it is fobidden to set pypi_index_url via @op so far, stay tuned for updates'

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

        # yep, create lazy constructor and return it
        # instead of function
        return wrap_call(
            f,
            output_types=output_types,
            provisioning=provisioning,
            env=env,
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
        self.__env_provider = py_env_provider

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
        py_env_provider: Optional[PyEnvProvider] = None,
        python_version: Optional[str] = None,
        libraries: Optional[Dict[str, str]] = None,
        local_modules_path: Optional[Sequence[str]] = None,
        exclude_packages: Iterable[str] = (),
        pypi_index_url: Optional[str] = None,
        conda_yaml_path: Optional[str] = None,
        docker_image: Optional[str] = None,
        docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
        interactive: bool = True,
        provisioning: Optional[Provisioning] = None,
        cpu_type: StringRequirement = None,
        cpu_count: IntegerRequirement = None,
        gpu_type: StringRequirement = None,
        gpu_count: IntegerRequirement = None,
        ram_size_gb: IntegerRequirement = None,
        env_variables: Optional[Mapping[str, str]] = None,
        docker_credentials: Optional[DockerCredentials] = None,
        env: Optional[Env] = None,
    ) -> LzyWorkflow:
        self.__register_default_runtime_storage()

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

        env = env or Env()
        env = env.override(
            env_variables=env_variables,
            python_version=python_version,
            libraries=libraries,
            local_modules_path=local_modules_path,
            pypi_index_url=pypi_index_url,
            conda_yaml_path=conda_yaml_path,
            docker_image=docker_image,
            docker_pull_policy=docker_pull_policy,
            docker_credentials=docker_credentials,
        )
        # So we have next ways to obtain pypi_index_url and we have to respect it in next order:
        # 1) pypi_index_url from .workflow(pypi_index_url=...)
        # 2) pypi_index_url from .workflow(env=Env(...))
        # 3) pypi_index_url from Pip().index_url
        # 4) Absense of any set pypi_index_url, PYPI_SIMPLE_ENDPOINT in this case
        # TODO: inсapsulate this logic into Env() (after moving logic of EnvProvider into Env)
        env.pypi_index_url = env.pypi_index_url or Pip().index_url or PYPI_SIMPLE_ENDPOINT
        env.validate()

        env_provider = (
            py_env_provider or
            self.__env_provider or
            AutomaticPyEnvProvider(pypi_index_url=env.pypi_index_url)
        )

        if (
            env.pypi_index_url and
            env_provider.pypi_index_url and
            env.pypi_index_url != env_provider.pypi_index_url
        ):
            # NB: if user passes his own py_env_provider, pypi_index_url could
            # differ from pypi_index_url obtained via env.
            # This mess should go away after incapsulating PyEnvProvider into Env
            raise RuntimeError(
                f"mismatch of pypi_index_url at py_env_provider and at env: "
                f"{env_provider.pypi_index_url} vs {env.pypi_index_url}"
            )

        auto_py_env = env_provider.provide(namespace, exclude_packages)

        return LzyWorkflow(
            name,
            self,
            env=env,
            auto_py_env=auto_py_env,
            provisioning=provisioning,
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
