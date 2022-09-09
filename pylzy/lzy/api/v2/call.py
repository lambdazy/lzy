import functools
import typing
import uuid
from typing import Any, Callable, Dict, Iterator, Mapping, Sequence, Tuple, TypeVar

from lzy.api.v2.env import Env
from lzy.api.v2.provisioning import Provisioning
from lzy.api.v2.signatures import CallSignature
from lzy.api.v2.utils.proxy_adapter import is_lzy_proxy, lzy_proxy
from lzy.api.v2.utils.types import infer_call_signature
from lzy.api.v2.workflow import LzyWorkflow

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall:
    def __init__(
        self,
        parent_wflow: LzyWorkflow,
        sign: CallSignature,
        provisioning: Provisioning,
        env: Env,
    ):
        self.__id = str(uuid.uuid4())
        self.__wflow = parent_wflow
        self.__sign = sign
        self.__provisioning = provisioning
        self.__env = env
        self.__entry_ids = [
            parent_wflow.snapshot.create_entry(typ).id for typ in sign.func.output_types
        ]

        self.__args_entry_ids: typing.List[str] = []

        for arg in self.__sign.args:
            if is_lzy_proxy(arg):
                self.__args_entry_ids.append(arg.__lzy_entry_id__)
            else:
                self.__args_entry_ids.append(
                    parent_wflow.snapshot.create_entry(type(arg)).id
                )

        self.__kwargs_entry_ids: Dict[str, str] = {}

        for name, kwarg in self.__sign.kwargs.items():
            entry_id: str
            if is_lzy_proxy(kwarg):
                entry_id = kwarg.__lzy_entry_id__
            else:
                entry_id = parent_wflow.snapshot.create_entry(type(kwarg)).id

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
    def operation_name(self) -> str:
        return self.__sign.func.name

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
        return self.kwarg_entry_ids

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.__sign.kwargs

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return self.__sign.named_arguments()

    @property
    def description(self) -> str:
        return self.__sign.description  # TODO(artolord) Add arguments description here


def wrap_call(
    f: Callable[..., Any],
    output_types: Sequence[type],
    provisioning_: Provisioning,
    env: Env,
    workflow: LzyWorkflow,
) -> Callable[..., Any]:
    @functools.wraps(f)
    def lazy(*args, **kwargs):
        # TODO: defaults?

        signature = infer_call_signature(f, output_types, *args, **kwargs)
        lzy_call = LzyCall(workflow, signature, provisioning_, env)
        workflow.register_call(lzy_call)

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
            if issubclass(output_types[0], type(None)):
                return None
            return lzy_proxy(
                lzy_call.entry_ids[0],
                lzy_call.signature.func.output_types[0],
                lzy_call.parent_wflow,
            )

        return tuple(
            lzy_proxy(
                lzy_call.entry_ids[i],
                lzy_call.signature.func.output_types[i],
                lzy_call.parent_wflow,
            )
            for i in range(len(lzy_call.entry_ids))
        )

    return lazy
