from typing import Callable, Any


class Wrapper:
    def __init__(self, func: Callable):
        self._func = func
        self._unwrap = None

    def __getattr__(self, attr):
        if self._unwrap is None:
            self._unwrap = self._func()
        return getattr(self._unwrap, attr)


def op(func: Callable) -> Callable:
    def lazy(*args):
        return Wrapper(func)

    return lazy


class LzyEnvironment:
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self):
        print("")


class Bus:
    pass


class KeyedIteratorBus(Bus):
    def __init__(self, key_extractor: Callable[[Any], str]):
        self._key_extractor = key_extractor


class LzyEnvironmentBuilder:
    def bus(self, func: Callable, bus: Bus):
        return self

    def build(self) -> LzyEnvironment:
        return LzyEnvironment()
