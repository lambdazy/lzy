from typing import Callable, Any


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
        return Wrapper(func, *args)

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
