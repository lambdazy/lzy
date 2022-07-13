import uuid
import typing

from itertools import chain
from typing import Any, Dict, Generic, Iterator, Tuple, TypeVar

from lzy.api.v2.servant.model.signatures import CallSignature
from lzy.proto.bet.priv.v2 import Zygote

if typing.TYPE_CHECKING:
    from lzy.api.v2.api.lzy_workflow import LzyWorkflow

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall(Generic[T]):
    def __init__(
        self,
        zygote: Zygote,
        parent_wflow: LzyWorkflow,
        sign: CallSignature[T],
        entry_id: str,
    ):
        self._id = str(uuid.uuid4())
        self._zygote = zygote
        self._sign = sign
        self._entry_id = entry_id
        self._wflow = parent_wflow

    @property
    def zygote(self) -> Zygote:
        return self._zygote

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
            zip(self._sign.func.param_names, self._args),
            # named arguments
            self._kwargs.items(),
        )

    @property
    def description(self) -> str:
        return self._sign.description  # TODO(artolord) Add arguments description here
