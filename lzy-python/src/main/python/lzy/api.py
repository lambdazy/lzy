import inspect
from typing import Callable, Any, get_type_hints, Iterable


class LzyOp:
    def __init__(self, func: Callable, *args):
        self._func = func
        self._args = args
        self._materialized = False
        self._materialization = None

    def __getattr__(self, attr):
        return self.materialize().__getattr__(attr)

    def __str__(self):
        return self.materialize().__str__()

    def __iter__(self):
        return self.materialize().__iter__()

    def __index__(self):
        return self.materialize().__index__()

    def materialize(self) -> Any:
        if not self._materialized:
            self._materialization = self._func(*self._args)
            self._materialized = True
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized

    def func(self) -> Callable:
        return self._func


def op(func: Callable) -> Callable:
    def lazy(*args) -> Any:
        env = None
        for stack in inspect.stack():
            lcls = stack.frame.f_locals
            for k, v in lcls.items():
                if type(v) == LzyEnvironment and v.active():
                    if env is None:
                        env = v
                    else:
                        raise ValueError('More than one started lzy environment found')

        if env is None:
            raise ValueError('Started lzy environment not found')
        wrapper = LzyOp(func, *args)
        env.register(wrapper)
        return wrapper

    return lazy


class LzyEnvironment:
    def __init__(self, eager: bool):
        self._wrappers = []
        self._entered = False
        self._exited = False
        self._eager = eager

    def __enter__(self):
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.run()
        self._exited = True

    def active(self) -> bool:
        return self._entered and not self._exited

    def register(self, wrapper: LzyOp) -> None:
        self._wrappers.append(wrapper)
        if self._eager:
            wrapper.materialize()

    def registered_ops(self) -> Iterable[LzyOp]:
        if not self._entered:
            raise ValueError('Fetching ops on a non-entered environment')
        return list(self._wrappers)

    def run(self) -> None:
        if not self._entered:
            raise ValueError('Run operation on a non-entered environment')
        if self._exited:
            raise ValueError('Run operation on an exited environment')
        # noinspection PyTypeChecker
        if len(self._wrappers) == 0:
            raise ValueError('No registered ops')
        for wrapper in self._wrappers:
            wrapper.materialize()


class LzyUtils:
    @staticmethod
    def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
        for op in ops:
            inp = "("
            ret = ""
            hints = get_type_hints(op.func())
            keys = list(hints.keys())
            for i in range(len(keys)):
                if keys[i] != 'return':
                    inp += str(hints[keys[i]])
                    inp += ","
                else:
                    ret += str(hints[keys[i]])
            inp += ")"
            print(
                inp + " -> " + str(op.func()) + " -> " + ret + ", materialized=" + str(op.is_materialized()))


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
