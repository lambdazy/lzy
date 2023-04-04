from dataclasses import dataclass
from typing import Generic, TypeVar, Union

T = TypeVar("T")  # pylint: disable=invalid-name
U = TypeVar("U")  # pylint: disable=invalid-name


@dataclass
class Left(Generic[T]):
    value: T


@dataclass
class Right(Generic[U]):
    value: U


Either = Union[Left[T], Right[U]]
