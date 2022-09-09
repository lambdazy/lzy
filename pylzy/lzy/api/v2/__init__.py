import inspect
import logging
import sys
from typing import Any, Callable, Dict, Iterator, Optional, Sequence, TypeVar

from lzy.api.v2.call import LzyCall, wrap_call
from lzy.api.v2.env import CondaEnv, DockerEnv, DockerPullPolicy, Env
from lzy.api.v2.local.runtime import LocalRuntime
from lzy.api.v2.provisioning import Gpu, Provisioning
from lzy.api.v2.query import Query
from lzy.api.v2.runtime import Runtime
from lzy.api.v2.snapshot import DefaultSnapshot
from lzy.api.v2.utils.conda import generate_conda_yaml
from lzy.api.v2.utils.env import generate_env, merge_envs
from lzy.api.v2.utils.packages import to_str_version
from lzy.api.v2.utils.proxy_adapter import lzy_proxy
from lzy.api.v2.utils.types import infer_call_signature, infer_return_type
from lzy.api.v2.whiteboard_declaration import whiteboard_
from lzy.api.v2.workflow import LzyWorkflow
from lzy.proxy.result import Nothing
from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider
from lzy.serialization.api import SerializerRegistry
from lzy.serialization.registry import DefaultSerializerRegistry
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

        active_workflow: Optional[LzyWorkflow] = LzyWorkflow.get_active()
        if active_workflow is None:
            return f

        generated_env = generate_env(
            active_workflow.auto_py_env,
            python_version,
            libraries,
            conda_yaml_path,
            docker_image,
            docker_pull_policy,
            local_modules_path,
        )
        merged_env = merge_envs(generated_env, active_workflow.default_env)
        # yep, create lazy constructor and return it
        # instead of function
        return wrap_call(f, output_types, provisioning_, merged_env, active_workflow)

    provisioning_ = Provisioning()  # TODO (tomato): update provisioning

    if func is None:
        return deco

    return deco(func)


def whiteboard(name: str):
    def wrap(cls):
        return whiteboard_(cls, "default", name)

    return wrap


class Lzy:
    # noinspection PyShadowingNames
    def __init__(
        self,
        *,
        runtime: Runtime = LocalRuntime(),
        py_env_provider: PyEnvProvider = AutomaticPyEnvProvider(),
        storage_registry: StorageRegistry = DefaultStorageRegistry(),
        serializer_registry: SerializerRegistry = DefaultSerializerRegistry(),
    ):
        self.__env_provider = py_env_provider
        self.__runtime = runtime
        self.__serializer_registry = serializer_registry
        self.__storage_registry = storage_registry

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
        *,
        eager: bool = False,
        python_version: Optional[str] = None,
        libraries: Optional[Dict[str, str]] = None,
        conda_yaml_path: Optional[str] = None,
        docker_image: Optional[str] = None,
        docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
        local_modules_path: Optional[Sequence[str]] = None,
    ) -> LzyWorkflow:
        namespace = inspect.stack()[1].frame.f_globals
        return LzyWorkflow(
            name,
            self,
            namespace,
            eager=eager,
            python_version=python_version,
            libraries=libraries,
            conda_yaml_path=conda_yaml_path,
            docker_image=docker_image,
            docker_pull_policy=docker_pull_policy,
            local_modules_path=local_modules_path,
        )

    # register cloud injections
    # noinspection PyBroadException
    try:
        from lzy.injections import catboost_injection
    except:
        pass
