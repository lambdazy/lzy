from typing import Callable, Any
import inspect


class Wrapper:
    def __init__(self, func: Callable, *args):
        self._func = func
        self._args = args
        self._unwrap = None

    def __getattr__(self, attr):
        return self.materialize().__getattr__(attr)

    def __str__(self):
        return self.materialize().__str__()

    def __iter__(self):
        return self.materialize().__iter__()

    def materialize(self):
        if self._unwrap is None:
            self._unwrap = self._func(*self._args)
        return self._unwrap


def op(func: Callable) -> Callable:
    def lazy(*args):
        env = None
        for stack in inspect.stack():
            lcls = stack.frame.f_locals
            for k, v in lcls.items():
                if type(v) == LzyEnvironment and v.entered():
                    if env is None:
                        env = v
                    else:
                        raise ValueError('More than one started lzy environment found')

        if env is None:
            raise ValueError('Started lzy environment not found')
        wrapper = Wrapper(func, *args)
        env.register(wrapper)
        return wrapper

    return lazy


class LzyEnvironment:
    def __init__(self, eager: bool):
        self._wrappers = []
        self._started = False
        self._eager = eager

    def __enter__(self):
        self._started = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.run()
        self._started = False

    def entered(self):
        return self._started

    def register(self, wrapper: Wrapper) -> None:
        self._wrappers.append(wrapper)
        if self._eager:
            wrapper.materialize()

    def run(self):
        if not self._started:
            raise ValueError('Run operation on a non-entered environment')
        # noinspection PyTypeChecker
        if len(self._wrappers) == 0:
            raise ValueError('No registered ops')
        self._wrappers[len(self._wrappers) - 1].materialize()


class Bus:
    pass


class KeyedIteratorBus(Bus):
    def __init__(self, key_extractor: Callable[[Any], str]):
        super().__init__()
        self._key_extractor = key_extractor


class LzyEnvironmentBuilder:
    def __init__(self):
        self._eager = False

    def bus(self, func: Callable, bus: Bus):
        return self

    def eager(self):
        self._eager = True
        return self

    def build(self) -> LzyEnvironment:
        return LzyEnvironment(self._eager)
