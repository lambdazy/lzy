from __future__ import annotations

from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES
from typing_extensions import Literal


def update_wrapper(
    wrapper,
    wrapped,
    assigned = WRAPPER_ASSIGNMENTS,
    updated = WRAPPER_UPDATES
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


class kwargsdispatchmethod:
    def __init__(self, func):
        if not callable(func) and not hasattr(func, "__get__"):
            raise TypeError(f"{func!r} is not callable or a descriptor")

        self.kwargs_func = None
        self.args_func = None
        self.func = func

    def register(self, type: Literal['args', 'kwargs']):
        def registrator(func):
            if type == 'args':
                self.args_func = func
            else:
                self.kwargs_func = func

            return func

        return registrator

    def __get__(self, obj, cls):
        def _method(*args, **kwargs):
            if args and kwargs:
                raise RuntimeError('use positional args or keyword arguments but not both')
            method = self.kwargs_func
            if args:
                method = self.args_func

            return method.__get__(obj, cls)(*args, **kwargs)

        _method.__isabstractmethod__ = self.__isabstractmethod__
        _method.register = self.register
        update_wrapper(_method, self.func)
        return _method

    @property
    def __isabstractmethod__(self):
        return getattr(self.func, "__isabstractmethod__", False)
