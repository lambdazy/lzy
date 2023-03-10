import asyncio
import functools
import inspect
import uuid
from inspect import getfullargspec
from itertools import chain, zip_longest
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple, TypeVar, Optional, List, Type

from serialzy.api import SerializerRegistry
# noinspection PyProtectedMember
from serialzy.types import get_type

from lzy.api.v1.env import Env
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.snapshot import Snapshot
from lzy.api.v1.utils.proxy_adapter import lzy_proxy, materialize, is_lzy_proxy, get_proxy_entry_id
from lzy.api.v1.utils.types import infer_real_types, get_default_args, check_types_serialization_compatible, is_subtype
from lzy.api.v1.workflow import LzyWorkflow
from lzy.utils.event_loop import LzyEventLoop

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall:
    def __init__(
        self,
        workflow: LzyWorkflow,
        sign: CallSignature,
        provisioning: Provisioning,
        env: Env,
        description: str = "",
        version: str = "0.0",
        cache: bool = False,
        lazy_arguments: bool = False
    ):
        self.__lazy_arguments = lazy_arguments
        self.__id = str(uuid.uuid4())
        self.__wflow = workflow
        self.__sign = sign
        self.__provisioning = provisioning
        self.__env = env
        self.__description = description
        self.__version = version
        self.__cache = cache

        local_data_put_tasks = []

        self.__args_entry_ids: List[str] = []
        for i, arg in enumerate(sign.args):
            if is_lzy_proxy(arg):
                eid = get_proxy_entry_id(arg)
            else:
                arg_name = sign.func.arg_names[i]
                eid = workflow.snapshot.create_entry(name=sign.func.callable.__name__ + "." + arg_name,
                                                     typ=sign.func.input_types[arg_name]).id
                local_data_put_tasks.append(self.__wflow.snapshot.put_data(eid, arg))
            self.__args_entry_ids.append(eid)

        self.__kwargs_entry_ids: Dict[str, str] = {}
        for kwarg_name, kwarg in sign.kwargs.items():
            if is_lzy_proxy(kwarg):
                eid = get_proxy_entry_id(kwarg)
            else:
                eid = workflow.snapshot.create_entry(name=sign.func.callable.__name__ + "." + kwarg_name,
                                                     typ=sign.func.input_types[kwarg_name]).id
                local_data_put_tasks.append(self.__wflow.snapshot.put_data(eid, kwarg))
            self.__kwargs_entry_ids[kwarg_name] = eid

        self.__entry_ids: List[str] = []
        for i, arg_typ in enumerate(sign.func.output_types):
            name = sign.func.callable.__name__ + ".return_" + str(i)
            eid = workflow.snapshot.create_entry(name, arg_typ).id
            self.__entry_ids.append(eid)

        # yep, we should store local data to storage just in LzyCall.__init__
        # because the data can be changed before dependent op will be actually executed
        LzyEventLoop.gather(*local_data_put_tasks)

    @property
    def provisioning(self) -> Provisioning:
        return self.__provisioning

    @property
    def env(self) -> Env:
        return self.__env

    @property
    def parent_wflow(self) -> LzyWorkflow:
        return self.__wflow

    @property
    def signature(self) -> CallSignature:
        return self.__sign

    @property
    def id(self) -> str:
        return self.__id

    @property
    def entry_ids(self) -> Sequence[str]:
        return self.__entry_ids

    @property
    def args(self) -> Tuple[Any, ...]:
        return self.__sign.args

    @property
    def arg_entry_ids(self) -> Sequence[str]:
        return self.__args_entry_ids

    @property
    def kwarg_entry_ids(self) -> Mapping[str, str]:
        return self.__kwargs_entry_ids

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.__sign.kwargs

    @property
    def description(self) -> str:
        return self.__description

    @property
    def lazy_arguments(self) -> bool:
        return self.__lazy_arguments

    @property
    def version(self) -> str:
        return self.__version

    @property
    def cache(self) -> bool:
        return self.__cache


def wrap_call(
    f: Callable[..., Any],
    output_types: Sequence[type],
    provisioning: Provisioning,
    env: Env,
    description: str = "",
    version: str = "0.0",
    cache: bool = False,
    lazy_arguments: bool = False
) -> Callable[..., Any]:
    @functools.wraps(f)
    def lazy(*args, **kwargs):

        active_workflow: Optional[LzyWorkflow] = LzyWorkflow.get_active()
        if active_workflow is None:
            return f(*args, **kwargs)

        prov = active_workflow.provisioning.override(provisioning)
        prov.validate()

        env_updated = active_workflow.env.override(env)
        if env_updated.conda_yaml_path is None:
            # it is guaranteed that PyEnv is not None if conda_yaml_path is None
            py_env = active_workflow.auto_py_env
            env_updated = active_workflow.env.override(Env(
                py_env.python_version, py_env.libraries, None, None, None, py_env.local_modules_path
            )).override(env)
        env_updated.validate()

        signature = infer_and_validate_call_signature(f, output_types, active_workflow.snapshot,
                                                      active_workflow.owner.serializer_registry, *args, **kwargs)
        lzy_call = LzyCall(active_workflow, signature, prov, env_updated, description, version, cache, lazy_arguments)
        active_workflow.register_call(lzy_call)

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
        if len(output_types) == 1:
            if inspect.isclass(output_types[0]) and issubclass(output_types[0], type(None)):
                return None
            # noinspection PyTypeChecker
            proxy = lzy_proxy(lzy_call.entry_ids[0], infer_real_types(lzy_call.signature.func.output_types[0]),
                              lzy_call.parent_wflow)
            if active_workflow.eager:
                return materialize(proxy)
            return proxy

        # noinspection PyTypeChecker
        proxies = tuple(
            lzy_proxy(
                lzy_call.entry_ids[i],
                infer_real_types(lzy_call.signature.func.output_types[i]),
                lzy_call.parent_wflow,
            )
            for i in range(len(lzy_call.entry_ids))
        )
        if active_workflow.eager:
            return tuple(materialize(p) for p in proxies)
        return proxies

    return lazy


def infer_and_validate_call_signature(
    f: Callable, output_type: Sequence[type],
    snapshot: Snapshot,
    serializer_registry: SerializerRegistry,
    *args, **kwargs
) -> CallSignature:
    types_mapping = {}
    args_mapping = {}
    argspec = getfullargspec(f)

    if argspec.varkw is None:
        for kwarg in kwargs.keys():
            if kwarg not in argspec.annotations:
                raise KeyError(f"Unexpected key argument {kwarg}")

    defaults = get_default_args(f)
    args_filled = [i for i in range(len(args))]
    for spec_name, real_arg in zip_longest(argspec.args, args_filled):
        if spec_name is not None and real_arg is None and spec_name not in kwargs and spec_name not in defaults:
            raise KeyError(f"Argument {spec_name} is required but not provided")
        elif argspec.varargs is None and real_arg is not None and spec_name is None:
            raise KeyError(f"Unexpected argument at position {real_arg}")

    for name, arg in chain(zip(argspec.args, args), kwargs.items()):
        # noinspection PyProtectedMember
        entry_type: Optional[Type] = None
        if is_lzy_proxy(arg):
            eid = get_proxy_entry_id(arg)
            entry_type = snapshot.get(eid).typ
            types_mapping[name] = entry_type
        elif name in argspec.annotations:
            types_mapping[name] = argspec.annotations[name]
        else:
            types_mapping[name] = get_type(arg)

        if name in argspec.annotations:
            typ = entry_type if entry_type else get_type(arg)
            compatible = check_types_serialization_compatible(argspec.annotations[name], typ, serializer_registry)
            if not compatible or not is_subtype(typ, argspec.annotations[name]):
                raise TypeError(
                    f"Invalid types: argument {name} has type {argspec.annotations[name]} "
                    f"but passed type {typ}")

        args_mapping[name] = arg

    generated_names = []
    for arg in args[len(argspec.args):]:
        name = str(uuid.uuid4())[:8]
        generated_names.append(name)
        if is_lzy_proxy(arg):
            eid = get_proxy_entry_id(arg)
            entry = snapshot.get(eid)
            types_mapping[name] = entry.typ
        else:
            types_mapping[name] = get_type(arg)

    arg_names = tuple(argspec.args[: len(args)] + generated_names)
    kwarg_names = tuple(kwargs.keys())
    return CallSignature(
        FuncSignature(f, types_mapping, output_type, arg_names, kwarg_names),
        args,
        kwargs,
    )
