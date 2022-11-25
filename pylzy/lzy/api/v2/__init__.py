import inspect
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar

from lzy.api.v2.call import LzyCall, wrap_call
from lzy.api.v2.env import CondaEnv, DockerEnv, DockerPullPolicy, Env
from lzy.api.v2.local.runtime import LocalRuntime
from lzy.api.v2.provisioning import Provisioning
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
from lzy.api.v2.runtime import Runtime
from lzy.serialization.registry import LzySerializerRegistry
from lzy.api.v2.snapshot import DefaultSnapshot
from lzy.api.v2.utils.conda import generate_conda_yaml
from lzy.api.v2.utils.env import generate_env, merge_envs
from lzy.api.v2.utils.packages import to_str_version
from lzy.api.v2.utils.proxy_adapter import lzy_proxy
from lzy.api.v2.utils.types import infer_call_signature, infer_return_type
from lzy.whiteboards.whiteboard import WhiteboardRepository
from lzy.whiteboards.whiteboard_declaration import whiteboard_
from lzy.api.v2.workflow import LzyWorkflow
from lzy.proxy.result import Nothing
from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider
from serialzy.api import SerializerRegistry
from lzy.storage.api import StorageRegistry
from lzy.storage.registry import DefaultStorageRegistry

T = TypeVar("T")  # pylint: disable=invalid-name

FuncT = TypeVar(
    "FuncT",
    bound=Callable[..., Any],
)


# pylint: disable=[invalid-name]
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
    provisioning_: Provisioning = Provisioning(),
    cpu_type: Optional[str] = None,
    cpu_count: Optional[int] = None,
    gpu_type: Optional[str] = None,
    gpu_count: Optional[int] = None,
    ram_size_gb: Optional[int] = None,
    env: Optional[Env] = None,
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

        # yep, create lazy constructor and return it
        # instead of function
        return wrap_call(f, output_types, python_version, libraries, conda_yaml_path, docker_image, docker_pull_policy,
                         local_modules_path, provisioning_, cpu_type, cpu_count, gpu_type, gpu_count, ram_size_gb, env)

    if func is None:
        return deco

    return deco(func)


def whiteboard(name: str, namespace: str = None):
    def wrap(cls):
        return whiteboard_(cls, "default", name)

    return wrap


class Lzy:
    # noinspection PyShadowingNames
    def __init__(
        self,
        *,
        runtime: Runtime = GrpcRuntime(),
        py_env_provider: PyEnvProvider = AutomaticPyEnvProvider(),
        storage_registry: StorageRegistry = DefaultStorageRegistry(),
        serializer_registry: SerializerRegistry = LzySerializerRegistry(),
        whiteboard_repository: Optional[WhiteboardRepository] = None
    ):
        self.__env_provider = py_env_provider
        self.__runtime = runtime
        self.__serializer_registry = serializer_registry
        self.__storage_registry = storage_registry
        self.__whiteboard_repository = whiteboard_repository if whiteboard_repository is not None \
            else WhiteboardRepository.with_grpc_client(storage_registry, serializer_registry)

    @property
    def serializer(self) -> SerializerRegistry:
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
    def whiteboard_repository(self) -> WhiteboardRepository:
        return self.__whiteboard_repository

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
        env: Optional[Env] = None,
    ) -> LzyWorkflow:
        namespace = inspect.stack()[1].frame.f_globals
        if env is None:
            env = generate_env(
                self.env_provider.provide(namespace),
                python_version,
                libraries,
                conda_yaml_path,
                docker_image,
                docker_pull_policy,
                local_modules_path,
            )
        return LzyWorkflow(
            name,
            self,
            namespace,
            snapshot=DefaultSnapshot(
                storage_registry=self.storage_registry,
                serializer_registry=self.serializer,
            ),
            env=env,
            eager=eager,
            provisioning=provisioning.override(
                Provisioning(cpu_type, cpu_count, gpu_type, gpu_count, ram_size_gb)
            ),
            interactive=interactive,
        )


try:
    from lzy.injections import catboost_injection
except:
    pass
