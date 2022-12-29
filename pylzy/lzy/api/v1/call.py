import dataclasses
import functools
import inspect
import typing
import uuid
from inspect import getfullargspec
from itertools import chain
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple, TypeVar

# noinspection PyProtectedMember
from pydantic.decorator import ValidatedFunction
from serialzy.types import get_type

from lzy.api.v1.env import Env
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.snapshot import Snapshot
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, lzy_proxy, get_proxy_entry_id
from lzy.api.v1.utils.types import infer_real_types
from lzy.api.v1.workflow import LzyWorkflow
from lzy.py_env.api import PyEnv

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall:
    def __init__(
        self,
        parent_wflow: LzyWorkflow,
        sign: CallSignature,
        provisioning: Provisioning,
        env: Env,
        description: str = ""
    ):
        self.__id = str(uuid.uuid4())
        self.__wflow = parent_wflow
        self.__sign = sign
        self.__provisioning = provisioning
        self.__env = env
        self.__description = description
        self.__entry_ids = [parent_wflow.snapshot.create_entry(typ).id for typ in sign.func.output_types]

        self.__args_entry_ids: typing.List[str] = []
        for i, arg in enumerate(sign.args):
            if is_lzy_proxy(arg):
                self.__args_entry_ids.append(get_proxy_entry_id(arg))
            else:
                name = sign.func.arg_names[i]
                self.__args_entry_ids.append(parent_wflow.snapshot.create_entry(sign.func.input_types[name]).id)

        self.__kwargs_entry_ids: Dict[str, str] = {}
        for name, kwarg in sign.kwargs.items():
            entry_id: str
            if is_lzy_proxy(kwarg):
                entry_id = kwarg.__lzy_entry_id__
            else:
                entry_id = parent_wflow.snapshot.create_entry(sign.func.input_types[name]).id

            self.__kwargs_entry_ids[name] = entry_id

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


def wrap_call(
    f: Callable[..., Any],
    output_types: Sequence[type],
    provisioning: Provisioning,
    env: Env,
    description: str = ""
) -> Callable[..., Any]:
    @functools.wraps(f)
    def lazy(*args, **kwargs):

        active_workflow: typing.Optional[LzyWorkflow] = LzyWorkflow.get_active()
        if active_workflow is None:
            return f(*args, **kwargs)

        prov = active_workflow.provisioning.override(provisioning)
        prov.validate()

        env_updated = active_workflow.env.override(env)
        if env_updated.conda_yaml_path is None:
            # it is guaranteed that PyEnv is not None if conda_yaml_path is None
            py_env = typing.cast(PyEnv, active_workflow.auto_py_env)
            env_updated = active_workflow.env.override(Env(
                py_env.python_version, py_env.libraries, None, None, None, py_env.local_modules_path
            )).override(env)
        env_updated.validate()

        signature = infer_and_validate_call_signature(f, output_types, active_workflow.snapshot, *args, **kwargs)
        lzy_call = LzyCall(active_workflow, signature, prov, env_updated, description)
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
            return lzy_proxy(
                lzy_call.entry_ids[0],
                infer_real_types(lzy_call.signature.func.output_types[0]),
                lzy_call.parent_wflow,
            )

        # noinspection PyTypeChecker
        return tuple(
            lzy_proxy(
                lzy_call.entry_ids[i],
                infer_real_types(lzy_call.signature.func.output_types[i]),
                lzy_call.parent_wflow,
            )
            for i in range(len(lzy_call.entry_ids))
        )

    return lazy


def infer_and_validate_call_signature(
    f: Callable, output_type: Sequence[type], snapshot: Snapshot, *args, **kwargs
) -> CallSignature:
    types_mapping = {}
    argspec = getfullargspec(f)

    for typ in set(argspec.annotations.values()):
        if dataclasses.is_dataclass(typ):
            # do not validate dataclasses, because pydantic does not handle `default_factory` properly
            setattr(typ, "__get_validators__", lambda: [])

    vd = ValidatedFunction(f, {"arbitrary_types_allowed": True})
    # pylint: disable=protected-access
    for name, arg in chain(zip(argspec.args, args), kwargs.items()):
        # noinspection PyProtectedMember
        if is_lzy_proxy(arg):
            eid = get_proxy_entry_id(arg)
            entry = snapshot.get(eid)
            types_mapping[name] = entry.typ
            vd.model.__fields__[name].validators.clear()  # remove type validators for proxies to avoid materialization
        elif name in argspec.annotations:
            types_mapping[name] = argspec.annotations[name]
        else:
            types_mapping[name] = get_type(arg)

    vd.init_model_instance(*args, **kwargs)  # validate arguments

    generated_names = []
    for arg in args[len(argspec.args):]:
        name = str(uuid.uuid4())
        generated_names.append(name)
        # noinspection PyProtectedMember
        types_mapping[name] = (
            arg.lzy_call._op.output_type if is_lzy_proxy(arg) else get_type(arg)
        )

    arg_names = tuple(argspec.args[: len(args)] + generated_names)
    kwarg_names = tuple(kwargs.keys())
    return CallSignature(
        FuncSignature(f, types_mapping, output_type, arg_names, kwarg_names),
        args,
        kwargs,
    )
