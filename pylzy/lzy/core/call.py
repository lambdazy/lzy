from __future__ import annotations

import inspect
import uuid
from inspect import getfullargspec
from dataclasses import dataclass
from itertools import chain, zip_longest
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple,  Optional, List, TYPE_CHECKING

import yaml

from serialzy.api import SerializerRegistry
# noinspection PyProtectedMember
from serialzy.types import get_type

from lzy.env.provisioning.provisioning import Provisioning
from lzy.env.python.base import ModulePathsList, PackagesDict
from lzy.env.container.base import BaseContainer
from lzy.env.environment import LzyEnvironment, EnvVarsType
from lzy.env.mixin import WithEnvironmentMixin

from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.snapshot import Snapshot
from lzy.api.v1.utils.proxy_adapter import lzy_proxy, materialize, is_lzy_proxy, get_proxy_entry_id, materialized
from lzy.api.v1.utils.types import infer_real_types, get_default_args, check_types_serialization_compatible, is_subtype
from lzy.logs.config import get_logger
from lzy.utils.functools import update_wrapper
from lzy.utils.inspect import get_annotations
from lzy.utils.event_loop import LzyEventLoop

if TYPE_CHECKING:
    from lzy.core.workflow import LzyWorkflow


_LOG = get_logger(__name__)
_INSTALLED_VERSIONS = {"3.7.11": "py37", "3.8.12": "py38", "3.9.7": "py39"}


@dataclass
class LzyCall:
    workflow: LzyWorkflow
    signature: CallSignature
    call_env: LzyEnvironment
    description: str
    version: str
    cache: bool
    lazy_arguments: bool

    def __post_init__(self):
        self.id: str = str(uuid.uuid4())

        lzy_env = self.workflow.owner.env
        wf_env = self.workflow.env
        # NB: we must calculate combined env right after LazyCallWrapper calling
        # to prevent changing of lzy_env/workflow_env beetween calls affecting this call
        self.final_env: LzyEnvironment = lzy_env.combine(wf_env).combine(self.call_env)
        self.final_env.validate()

        sign = self.signature
        workflow = self.workflow

        local_data_put_tasks = []

        self.__args_entry_ids: List[str] = []
        for i, arg in enumerate(sign.args):
            if is_lzy_proxy(arg):
                eid = get_proxy_entry_id(arg)
            else:
                arg_name = sign.func.arg_names[i]
                eid = workflow.snapshot.create_entry(
                    name=self.callable_name + "." + arg_name,
                    typ=sign.func.input_types[arg_name]
                ).id
                local_data_put_tasks.append(workflow.snapshot.put_data(eid, arg))
            self.__args_entry_ids.append(eid)

        self.__kwargs_entry_ids: Dict[str, str] = {}
        for kwarg_name, kwarg in sign.kwargs.items():
            if is_lzy_proxy(kwarg):
                eid = get_proxy_entry_id(kwarg)
            else:
                eid = workflow.snapshot.create_entry(
                    name=self.callable_name + "." + kwarg_name,
                    typ=sign.func.input_types[kwarg_name]
                ).id
                local_data_put_tasks.append(workflow.snapshot.put_data(eid, kwarg))
            self.__kwargs_entry_ids[kwarg_name] = eid

        self.__entry_ids: List[str] = []
        for i, arg_typ in enumerate(sign.func.output_types):
            name = self.callable_name + f".return_{i}"
            eid = workflow.snapshot.create_entry(name, arg_typ).id
            self.__entry_ids.append(eid)

        exc_name = self.callable_name + ".exception"
        self.__exception_id: str = workflow.snapshot.create_entry(exc_name, tuple).id

        # yep, we should store local data to storage just in LzyCall.__init__
        # because the data can be changed before dependent op will be actually executed
        LzyEventLoop.gather(*local_data_put_tasks)

    @property
    def callable_name(self) -> str:
        return self.signature.func.callable.__name__

    @property
    def args(self) -> Tuple[Any, ...]:
        return self.signature.args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.signature.kwargs

    @property
    def arg_entry_ids(self) -> Sequence[str]:
        return self.__args_entry_ids

    @property
    def kwarg_entry_ids(self) -> Mapping[str, str]:
        return self.__kwargs_entry_ids

    @property
    def entry_ids(self) -> Sequence[str]:
        return self.__entry_ids

    @property
    def exception_id(self) -> str:
        return self.__exception_id

    def get_python_version(self) -> str:
        return self.final_env.get_python_env().get_python_version()

    def get_local_module_paths(self) -> ModulePathsList:
        return self.final_env.get_python_env().get_local_module_paths(
            self.workflow.env.get_namespace()
        )

    def get_pypi_packages(self) -> PackagesDict:
        return self.final_env.get_python_env().get_pypi_packages(
            self.workflow.env.get_namespace()
        )

    def get_provisioning(self) -> Provisioning:
        return self.final_env.get_provisioning()

    def get_container(self) -> BaseContainer:
        return self.final_env.get_container()

    def get_env_vars(self) -> EnvVarsType:
        return self.final_env.get_env_vars()

    def generate_conda_config(self) -> Dict[str, Any]:
        python_env = self.final_env.get_python_env()
        python_version = python_env.get_python_version()
        dependencies: List[Any] = []

        if python_version in _INSTALLED_VERSIONS:
            env_name = _INSTALLED_VERSIONS[python_version]
        else:
            _LOG.warning(
                f"Installed python version ({python_version}) is not cached remotely. "
                f"Usage of a cached python version ({list(_INSTALLED_VERSIONS.keys())}) "
                f"can decrease startup time."
            )
            dependencies = [f"python=={python_version}"]
            env_name = "default"

        dependencies.append("pip")

        extra_pip_options = [
            f'--index-url {python_env.get_pypi_index_url()}'
        ]

        libraries = [
            f"{name}=={version}"
            for name, version in self.get_pypi_packages().items()
        ]

        pip_options = extra_pip_options + libraries
        if pip_options:
            dependencies.append({"pip": pip_options})

        return {"name": env_name, "dependencies": dependencies}

    def get_conda_yaml(self) -> str:
        config = self.generate_conda_config()
        result = yaml.dump(config, sort_keys=False)
        return result


@dataclass(frozen=True)
class LazyCallWrapper(WithEnvironmentMixin):
    function: Callable[..., Any]
    output_types: Sequence[type]
    env: LzyEnvironment
    description: str
    version: str
    cache: bool
    lazy_arguments: bool

    def __post_init__(self):
        update_wrapper(self, self.function)

    def __call__(self, *args, **kwargs) -> Any:
        from lzy.core.workflow import LzyWorkflow

        active_workflow: Optional[LzyWorkflow] = LzyWorkflow.get_active()
        if active_workflow is None:
            return self.function(*args, **kwargs)

        signature = infer_and_validate_call_signature(
            self.function,
            *args,
            output_type=self.output_types,
            snapshot=active_workflow.snapshot,
            serializer_registry=active_workflow.owner.serializer_registry,
            **kwargs
        )

        lzy_call = LzyCall(
            workflow=active_workflow,
            signature=signature,
            call_env=self.env,
            description=self.description,
            version=self.version,
            cache=self.cache,
            lazy_arguments=self.lazy_arguments,
        )

        active_workflow.register_call(lzy_call)

        single_output = len(self.output_types) == 1

        # Special case for NoneType, just leave op registered and return
        # the real None. LzyEnv later will materialize it anyway.
        #
        # Otherwise, `is` checks won't work, for example:
        # >>> @op
        # ... def op_none_operation() -> None:
        # ...      pass
        #
        # >>> obj = op_none_operation()
        # >>> obj is None
        # >>> False
        if (
            single_output and
            inspect.isclass(self.output_types[0]) and
            issubclass(self.output_types[0], type(None))
        ):
            return None

        output_types_ids = zip(lzy_call.entry_ids, lzy_call.signature.func.output_types)
        proxies = (
            lzy_proxy(
                entry_id=entry_id,
                types=infer_real_types(types),
                wflow=active_workflow
            ) for entry_id, types in output_types_ids
        )

        if active_workflow.eager:
            result = tuple(materialize(p) for p in proxies)
        else:
            result = tuple(proxies)

        if single_output:
            return result[0]
        return result


def infer_and_validate_call_signature(
    f: Callable,
    *args,
    output_type: Sequence[type],
    snapshot: Snapshot,
    serializer_registry: SerializerRegistry,
    **kwargs
) -> CallSignature:
    types_mapping = {}
    argspec = getfullargspec(f)
    annotations = get_annotations(f, eval_str=True)

    if argspec.varkw is None:
        for kwarg in kwargs:
            if kwarg not in annotations:
                raise KeyError(f"Unexpected key argument {kwarg}")

    defaults = get_default_args(f)
    args_filled = list(range(len(args)))
    for spec_name, real_arg in zip_longest(argspec.args, args_filled):
        if spec_name is not None and real_arg is None and spec_name not in kwargs and spec_name not in defaults:
            raise KeyError(f"Argument {spec_name} is required but not provided")
        if argspec.varargs is None and real_arg is not None and spec_name is None:
            raise KeyError(f"Unexpected argument at position {real_arg}")

    names_for_varargs = [str(uuid.uuid4())[:8] for _ in range(len(args) - len(argspec.args))]
    actual_args = [None] * len(args)
    actual_kwargs = {}

    for i, (name, arg) in enumerate(chain(zip(chain(argspec.args, names_for_varargs), args), kwargs.items())):
        from_varargs = len(argspec.args) < i < len(args)
        if from_varargs:
            inferred_type = __infer_type(snapshot, name, arg)
        else:
            inferred_type = __infer_type(snapshot, name, arg, annotations)
            if name in annotations:
                typ = inferred_type if is_lzy_proxy(arg) else get_type(arg)
                compatible = check_types_serialization_compatible(annotations[name], typ, serializer_registry)
                if not compatible or not is_subtype(typ, annotations[name]):
                    raise TypeError(
                        f"Invalid types: argument {name} has type {annotations[name]} "
                        f"but passed type {typ}")

        types_mapping[name] = inferred_type
        # materialize proxy considered as local data
        if i < len(args):
            actual_args[i] = materialize(arg) if is_lzy_proxy(arg) and materialized(arg) else arg
        else:
            actual_kwargs[name] = materialize(arg) if is_lzy_proxy(arg) and materialized(arg) else arg

    arg_names = tuple(argspec.args[: len(args)] + names_for_varargs)
    kwarg_names = tuple(kwargs.keys())
    return CallSignature(
        FuncSignature(f, types_mapping, output_type, arg_names, kwarg_names),
        tuple(actual_args),
        actual_kwargs,
    )


def __infer_type(snapshot: Snapshot, arg_name: str, arg: Any, type_annotations: Optional[Dict[str, Any]] = None):
    if is_lzy_proxy(arg):
        return snapshot.get(get_proxy_entry_id(arg)).typ
    if type_annotations and arg_name in type_annotations:
        return type_annotations[arg_name]
    return get_type(arg)
