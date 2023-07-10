from __future__ import annotations

from abc import ABC
from dataclasses import is_dataclass, fields
from typing import Dict, Any, TypeVar, Union
from typing_extensions import Self, TypeGuard


class NotSpecifiedType:
    """
    >>> NotSpecified = NotSpecifiedType()
    >>> NotSpecified or 1
    1
    >>> 1 and NotSpecified
    NotSpecified
    """
    def __repr__(self):
        return 'NotSpecified'

    def __bool__(self):
        return False


T = TypeVar('T')
NotSpecified = NotSpecifiedType()
EnvironmentField = Union[NotSpecifiedType, T]


# NB: you may say that val must be EnvironmentField type, but
# mypy doesn't working correctly in this case (at least on 3.7),
# I think it wants to see T in this line on both sides
def is_specified(val: Union[NotSpecifiedType, T]) -> TypeGuard[T]:
    return val is not NotSpecified


class Deconstructible(ABC):
    def deconstruct(self) -> Dict[str, Any]:
        if is_dataclass(self):
            # we need a shallow copy here, but dataclasses.asdict performing deep copy
            return dict((field.name, getattr(self, field.name)) for field in fields(self))

        # if a descendant is not a dataclass, it have to implement deconstruct
        raise NotImplementedError

    def combine(self, other: Self) -> Self:
        right: Dict[str, Any] = {
            key: value
            for key, value in other.deconstruct().items()
            if is_specified(value)
        }

        return self.with_fields(**right)

    def with_fields(self, **kwargs: Any) -> Self:
        right: Dict[str, Any] = dict(self.deconstruct(), **kwargs)

        return type(self)(**right)

    def __eq__(self, other) -> bool:
        if type(self) is not type(other):
            return False

        return self.deconstruct() == other.deconstruct()  # type: ignore

    def __lt__(self, other) -> bool:
        assert isinstance(other, Deconstructible)

        left = tuple(self.deconstruct().items())
        right = tuple(other.deconstruct().items())

        return left < right

    def __ne__(self, other) -> bool:
        return not self == other

    def __repr__(self) -> str:
        args = (f'{k}={v!r}' for k, v in self.deconstruct().items())
        return "{name}({args})".format(
            name=self.__class__.__name__,
            args=', '.join(args),
        )
