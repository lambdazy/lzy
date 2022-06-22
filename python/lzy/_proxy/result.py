from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
    Union,
)

T = TypeVar("T")  # pylint: disable=invalid-name


class Nothing:
    pass


@dataclass
class Just(Generic[T]):
    value: T


Result = Union[Just[T], Nothing]
