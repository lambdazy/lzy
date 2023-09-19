from __future__ import annotations

import logging

from abc import ABC
from dataclasses import is_dataclass, fields, dataclass
from typing import Dict, Any, TypeVar, Union, Optional, ClassVar
from typing_extensions import Self, TypeGuard, final

from lzy.logs.config import get_logger


@final
class NotSpecifiedType:
    """
    >>> NotSpecified = NotSpecifiedType()
    >>> NotSpecified or 1
    1
    >>> 1 and NotSpecified
    NotSpecified
    >>> {NotSpecified, NotSpecified}
    {NotSpecified}
    >>> NotSpecifiedType() == NotSpecified
    True
    """
    def __repr__(self):
        return 'NotSpecified'

    def __eq__(self, other):
        return type(self) is type(other)

    def __bool__(self):
        return False

    def __hash__(self):
        return 0


T = TypeVar('T')
NotSpecified = NotSpecifiedType()
EnvironmentField = Union[NotSpecifiedType, T]


# NB: you may say that val must be EnvironmentField type, but
# mypy doesn't work correctly in this case (at least on 3.7),
# I think it wants to see T in this line on both sides
def is_specified(val: Union[NotSpecifiedType, T]) -> TypeGuard[T]:
    return val is not NotSpecified


class Deconstructible(ABC):
    def deconstruct(self) -> Dict[str, Any]:
        if is_dataclass(self):
            # we need a shallow copy here, but dataclasses.asdict performing deep copy
            return dict(
                (field.name, getattr(self, field.name))
                for field in fields(self)
                if field.init
            )

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

    def clone(self) -> Self:
        return self.with_fields()

    def __eq__(self, other) -> bool:
        if type(self) is not type(other):
            return False

        return self.deconstruct() == other.deconstruct()  # type: ignore

    def __ne__(self, other) -> bool:
        return not self == other

    def __repr__(self) -> str:
        args = (f'{k}={v!r}' for k, v in self.deconstruct().items())
        return "{name}({args})".format(
            name=self.__class__.__name__,
            args=', '.join(args),
        )

    def __hash__(self) -> int:
        return hash(tuple(self.deconstruct().items()))


@dataclass
class WithLogger:
    logger: ClassVar[Optional[logging.Logger]] = None

    def __post_init__(self):
        if not self.logger:
            kls = self.__class__
            self.logger = get_logger(f'{kls.__module__}.{kls.__name__}')

    @property
    def log(self) -> logging.Logger:
        """
        This property required only for unwrap Optional type of logger
        """
        assert self.logger
        return self.logger
