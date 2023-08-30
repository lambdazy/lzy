from __future__ import annotations

from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES
from typing import Callable, Generic, TypeVar, Optional, cast
from typing_extensions import Literal


T = TypeVar('T')
CT = Callable[..., T]


def update_wrapper(
    wrapper,
    wrapped,
    assigned=WRAPPER_ASSIGNMENTS,
    updated=WRAPPER_UPDATES
):
    """
    This function is a copypaste of functools.uptade_wrapper with
    allowance to use frozen dataclass as wrapper
    """
    for attr in assigned:
        try:
            value = getattr(wrapped, attr)
        except AttributeError:
            pass
        else:
            object.__setattr__(wrapper, attr, value)

    for attr in updated:
        getattr(wrapper, attr).update(getattr(wrapped, attr, {}))

    object.__setattr__(wrapper, '__wrapped__', wrapped)

    return wrapper


class kwargsdispatchmethod(Generic[T]):
    def __init__(self, func: CT[T]) -> None:
        if not callable(func) or not hasattr(func, "__get__"):
            raise TypeError(f"{func!r} is not callable or a descriptor")

        self.kwargs_func: Optional[CT[T]] = None
        self.args_func: Optional[CT[T]] = None
        self.func: CT[T] = func

    def register(self, type_: Literal['args', 'kwargs']) -> Callable[[CT[T]], CT[T]]:
        def registrator(func: CT[T]) -> CT[T]:
            if type_ == 'args':
                self.args_func = func
            else:
                self.kwargs_func = func

            return func

        return registrator

    def __get__(self, obj, cls) -> CT[T]:
        def _method(*args, **kwargs) -> T:
            if args and kwargs:
                raise RuntimeError('use positional args or keyword arguments but not both')

            if args:
                assert self.args_func
                return cast(T, self.args_func.__get__(obj, cls)(*args))

            assert self.kwargs_func
            return cast(T, self.kwargs_func.__get__(obj, cls)(**kwargs))

        _method.__isabstractmethod__ = self.__isabstractmethod__  # type: ignore
        _method.register = self.register  # type: ignore
        update_wrapper(_method, self.func)
        return _method

    @property
    def __isabstractmethod__(self) -> bool:
        return getattr(self.func, "__isabstractmethod__", False)
