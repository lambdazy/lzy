import uuid
from itertools import chain
from typing import Any, Dict, Generic, Iterator, Tuple, TypeVar

from lzy.api.v2.servant.model.signatures import FuncSignature
from lzy.proto.bet.priv.v2 import Zygote

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall(Generic[T]):
    def __init__(
        self, zygote: Zygote, sign: FuncSignature[T] , args: Tuple[Any, ...], kwargs: Dict[str, Any], entry_id: str,
    ):
        self._id = str(uuid.uuid4())
        self._zygote = zygote
        self._sign = sign
        self._args = args
        self._kwargs = kwargs
        self._entry_id = entry_id

    @property
    def zygote(self) -> Zygote:
        return self._zygote

    @property
    def signature(self) -> FuncSignature[T]:
        return self._sign

    @property
    def id(self) -> str:
        return self._id

    @property
    def operation_name(self) -> str:
        return self._sign.name

    @property
    def entry_id(self) -> str:
        return self._entry_id

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return chain(zip(self._sign.param_names, self._args), self._kwargs.items())

    @property
    def description(self) -> str:
        return self._sign.description  # TODO(artolord) Add arguments description here
