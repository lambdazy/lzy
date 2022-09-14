from dataclasses import dataclass
from typing import Generic, TypeVar, Union

T = TypeVar("T")  # pylint: disable=invalid-name


class Nothing:
    pass


@dataclass
class Just(Generic[T]):
    value: T


Result = Union[Just[T], Nothing]


def unwrap(res: Union[Just[T], Nothing]) -> T:
    if isinstance(res, Nothing):
        raise AttributeError("Cannot unwrap result, it is None")
    return res.value
