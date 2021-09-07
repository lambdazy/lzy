import inspect
from dataclasses import dataclass
from typing import Callable, Any, Dict, Type, Iterable, _GenericAlias, get_type_hints


@dataclass
class DataPage:
    typ: _GenericAlias
    func: Callable
    depends_on: Iterable  # Iterable[DataPage]
    materialized: bool


class Wrapper:
    def __init__(self, func: Callable, *args):
        self._func = func
        self._args = args
        self._materialized = False
        self._materialization = None
        self._data_page = None

    def __getattr__(self, attr):
        return self.materialize().__getattr__(attr)

    def __str__(self):
        return self.materialize().__str__()

    def __iter__(self):
        return self.materialize().__iter__()

    def materialize(self) -> Any:
        if not self._materialized:
            self._materialization = self._func(*self._args)
            self._materialized = True
        return self._materialization

    def data_page(self) -> DataPage:
        return DataPage(get_type_hints(self._func)['return'], self._func,
                        list(map(lambda x: x.data_page(), list(self._args))), self._materialized)


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
        wrapper = Wrapper(func, *args)
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

    def register(self, wrapper: Wrapper) -> None:
        self._wrappers.append(wrapper)
        if self._eager:
            wrapper.materialize()

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

    def print_relations(self):
        if not self._entered:
            raise ValueError('Print relations on a non-entered environment')
        for wrapper in self._wrappers:
            data_page = wrapper.data_page()
            inp = ""
            depends_on = list(data_page.depends_on)
            for i in range(len(depends_on)):
                inp += str(depends_on[i].typ)
                if i < len(depends_on) - 1:
                    inp += ","
                else:
                    inp += " -> "

            print(inp + str(data_page.func) + " -> " + str(data_page.typ) + ", materialized=" + str(
                data_page.materialized))


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
