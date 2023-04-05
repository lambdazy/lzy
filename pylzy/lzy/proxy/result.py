from dataclasses import dataclass
from typing import Generic, TypeVar, Union

T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass
class Absence:
    cause: Union[Exception, None] = None


@dataclass
class Result(Generic[T]):
    value: T


Either = Union[Result[T], Absence]


def unwrap(res: Union[Result[T], Absence]) -> T:
    if isinstance(res, Absence):
        raise AttributeError(f"Cannot unwrap result, it is absent because of cause={res.cause!r}")
    return res.value
