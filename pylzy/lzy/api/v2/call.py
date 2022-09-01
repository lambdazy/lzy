import typing
import uuid
from itertools import chain
from typing import Any, Dict, Generic, Iterator, Tuple, TypeVar

from lzy.api.v2.provisioning import Provisioning
from lzy.api.v2.proxy_adapter import is_lzy_proxy
from lzy.api.v2.signatures import CallSignature
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env import EnvSpec

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall:
    def __init__(
        self,
        parent_wflow: LzyWorkflow,
        sign: CallSignature,
        provisioning: Provisioning,
        env: EnvSpec,
    ):
        self._id = str(uuid.uuid4())
        self._wflow = parent_wflow
        self._sign = sign
        self._provisioning = provisioning
        self._env = env
        self._entry_ids = [
            parent_wflow.owner.snapshot.create_entry(typ).id
            for typ in sign.func.output_types
        ]

        self._args_entry_ids: typing.List[str] = []

        for arg in self._sign.args:
            if is_lzy_proxy(arg):
                self._args_entry_ids.append(arg.__lzy_entry_id__)
            else:
                self._args_entry_ids.append(
                    parent_wflow.owner.snapshot.create_entry(type(arg)).id
                )

        self._kwargs_entry_ids: Dict[str, str] = {}

        for name, kwarg in self._sign.kwargs.items():
            entry_id: str
            if is_lzy_proxy(kwarg):
                entry_id = kwarg.__lzy_entry_id__
            else:
                entry_id = parent_wflow.owner.snapshot.create_entry(type(kwarg)).id

            self._kwargs_entry_ids[name] = entry_id

    @property
    def provisioning(self) -> Provisioning:
        return self._provisioning

    @property
    def env(self) -> EnvSpec:
        return self._env

    @property
    def parent_wflow(self) -> LzyWorkflow:
        return self._wflow

    @property
    def signature(self) -> CallSignature:
        return self._sign

    @property
    def id(self) -> str:
        return self._id

    @property
    def operation_name(self) -> str:
        return self._sign.func.name

    @property
    def entry_ids(self) -> typing.Sequence[str]:
        return self._entry_ids

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._sign.args

    @property
    def arg_entry_ids(self) -> typing.Sequence[str]:
        return self._args_entry_ids

    @property
    def kwarg_entry_ids(self) -> typing.Mapping[str, str]:
        return self.kwarg_entry_ids

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._sign.kwargs

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return self._sign.named_arguments()

    @property
    def description(self) -> str:
        return self._sign.description  # TODO(artolord) Add arguments description here
