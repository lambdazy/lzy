import datetime
import inspect
import os
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, Iterable

from serialzy.api import SerializerRegistry

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard
from lzy.api.v1.call import LzyCall, wrap_call
from lzy.api.v1.env import CondaEnv, DockerEnv, DockerPullPolicy, Env
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.remote.runtime import RemoteRuntime, USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV
from lzy.api.v1.runtime import Runtime
from lzy.api.v1.snapshot import DefaultSnapshot
from lzy.api.v1.utils.conda import generate_conda_yaml
from lzy.api.v1.utils.env import generate_env, merge_envs
from lzy.api.v1.utils.packages import to_str_version
from lzy.api.v1.utils.proxy_adapter import lzy_proxy
from lzy.api.v1.utils.types import infer_call_signature, infer_return_type
from lzy.api.v1.whiteboards import whiteboard_, ReadOnlyWhiteboard
from lzy.api.v1.workflow import LzyWorkflow
from lzy.proxy.result import Nothing
from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage.api import StorageRegistry
from lzy.storage.registry import DefaultStorageRegistry
from lzy.utils.event_loop import LzyEventLoop
from lzy.whiteboards.api import WhiteboardClient
from lzy.whiteboards.remote import RemoteWhiteboardClient, WB_USER_ENV, WB_KEY_PATH_ENV, WB_ENDPOINT_ENV

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


def whiteboard(name: str, *, namespace: str = "default"):
    def wrap(cls):
        return whiteboard_(cls, namespace, name)

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
            whiteboard_client: WhiteboardClient = RemoteWhiteboardClient(),
            py_env_provider: PyEnvProvider = AutomaticPyEnvProvider(),
            storage_registry: StorageRegistry = DefaultStorageRegistry(),
            serializer_registry: SerializerRegistry = LzySerializerRegistry()
    ):
        self.__env_provider = py_env_provider
        self.__whiteboard_client = whiteboard_client
        self.__serializer_registry = serializer_registry
        self.__storage_registry = storage_registry
        self.__runtime = runtime

    @staticmethod
    def auth(*, user: str, key_path: str, endpoint: Optional[str] = None,
             whiteboards_endpoint: Optional[str] = None) -> None:
        lzy_auth(user=user, key_path=key_path, endpoint=endpoint, whiteboards_endpoint=whiteboards_endpoint)

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
    def whiteboard_client(self) -> WhiteboardClient:
        return self.__whiteboard_client

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

    def whiteboard(self, wb_id: str) -> Any:
        wb: Whiteboard = LzyEventLoop.run_async(self.__whiteboard_client.get(wb_id))
        return LzyEventLoop.run_async(self.__build_whiteboard(wb))

    def whiteboards(self, *,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None):
        wbs: Iterable[Whiteboard] = LzyEventLoop.run_async(self.__whiteboard_client.list(
            name, tags, not_before, not_after
        ))
        wbs_to_build = [self.__build_whiteboard(wb) for wb in wbs]
        return list(LzyEventLoop.gather(*wbs_to_build))

    async def __build_whiteboard(self, wb: Whiteboard) -> Any:

        if wb.status != Whiteboard.FINALIZED:
            raise RuntimeError(f"Status of whiteboard with name {wb.name} is {wb.status}, but must be COMPLETED")

        return ReadOnlyWhiteboard(self.__storage_registry, self.__serializer_registry, wb)


# noinspection PyBroadException
try:
    from lzy.injections import catboost_injection
except:
    pass
