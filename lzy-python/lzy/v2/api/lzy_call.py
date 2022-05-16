import uuid
from itertools import chain
from typing import Tuple, Any, Dict, TypeVar, Generic, Iterator

from lzy.v2.api.lzy_op import LzyOp

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall(Generic[T]):
    def __init__(self, op: LzyOp, args: Tuple[Any, ...], kwargs: Dict[str, Any], entry_id: str):
        self._id = str(uuid.uuid4())
        self._op = op
        self._args = args
        self._kwargs = kwargs
        self._entry_id = entry_id

    @property
    def id(self) -> str:
        return self._id

    @property
    def op(self) -> LzyOp:
        return self._op

    @property
    def operation_name(self) -> str:
        return self._op.name

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
        for name, arg in chain(zip(self._op.arg_names, self._args), self._kwargs.items()):
            yield name, arg

    @property
    def description(self) -> str:
        return self._op.description  # TODO(artolord) Add arguments description here