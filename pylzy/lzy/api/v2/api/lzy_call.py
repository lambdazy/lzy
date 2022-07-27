import typing
import uuid
from itertools import chain
from typing import Any, Dict, Generic, Iterator, Tuple, TypeVar

from lzy.api.v2.api.lzy_workflow import LzyWorkflow
from lzy.api.v2.api.provisioning import Provisioning
from lzy.api.v2.servant.model.signatures import CallSignature
from lzy.env.env import EnvSpec

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall(Generic[T]):
    def __init__(
        self,
        parent_wflow: LzyWorkflow,
        sign: CallSignature[T],
        provisioning: Provisioning,
        env: EnvSpec,
        entry_id: str,
    ):
        self._id = str(uuid.uuid4())
        self._wflow = parent_wflow
        self._sign = sign
        self._provisioning = provisioning
        self._env = env
        self._entry_id = entry_id

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
    def signature(self) -> CallSignature[T]:
        return self._sign

    @property
    def id(self) -> str:
        return self._id

    @property
    def operation_name(self) -> str:
        return self._sign.func.name

    @property
    def entry_id(self) -> str:
        return self._entry_id

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._sign.args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._sign.kwargs

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return chain(
            # positional arguments
            zip(self._sign.func.param_names, self._sign.args),
            # named arguments
            self._kwargs.items(),
        )

    @property
    def description(self) -> str:
        return self._sign.description  # TODO(artolord) Add arguments description here
