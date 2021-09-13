import dataclasses
import inspect
from abc import abstractmethod
from typing import get_type_hints, List, Tuple, Callable, Type, Any, TypeVar, Iterable

from lzy.op import LzyOp
from lzy.proxy import Proxy
from lzy.whiteboard import WhiteboardsRepoInMem, WhiteboardControllerImpl


def op(func: Callable) -> Callable:
    hints = get_type_hints(func)
    if 'return' not in hints:
        raise ValueError('Op should be annotated with return type')

    def lazy(*args) -> Any:
        env = None
        for stack in inspect.stack():
            lcls = stack.frame.f_locals
            for k, v in lcls.items():
                if type(v) == LzyEnv and v.is_active():
                    if env is None:
                        env = v
                    else:
                        raise ValueError('More than one started lzy environment found')

        if env is None:
            return func(*args)
        else:
            return_hint = hints['return']
            if hasattr(return_hint, '__origin__'):
                return_type = return_hint.__origin__
            elif type(return_hint) == type:
                return_type = return_hint
            else:
                raise ValueError('Cannot infer op return type')

            wrapper = LzyOp(func, return_type, *args)
            env.register(wrapper)
            return wrapper

    return lazy


class Bus:
    pass


class KeyedIteratorBus(Bus):
    def __init__(self, key_extractor: Callable[[Any], str]):
        super().__init__()
        self._key_extractor = key_extractor


T = TypeVar('T')


class LzyEnvBase:
    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def register(self, lzy_op: LzyOp) -> None:
        pass

    @abstractmethod
    def registered_ops(self) -> Iterable[LzyOp]:
        pass

    @abstractmethod
    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        pass

    @abstractmethod
    def projections(self, typ: Type[T]) -> Iterable[T]:
        pass

    @abstractmethod
    def run(self) -> None:
        pass


class LzyEnv(LzyEnvBase):
    # noinspection PyDefaultArgument
    def __init__(self, eager: bool = False, whiteboard: Any = None, buses: List[Tuple[Callable, Bus]] = []):
        super().__init__()
        if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
            raise ValueError('Whiteboard should be a dataclass')
        if whiteboard is not None:
            self._wb_controller = WhiteboardControllerImpl(whiteboard)
        else:
            self._wb_controller = None

        self._wb_repo = WhiteboardsRepoInMem()
        self._ops = []
        self._entered = False
        self._exited = False
        self._eager = eager
        self._buses = list(buses)

    def __enter__(self):  # -> LzyEnv
        self._entered = True
        Proxy.cleanup()
        if self._wb_controller is not None:
            self._wb_controller.capture()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.run()
        if self._wb_controller is not None:
            self._wb_repo.register(self._wb_controller.finalize())
        self._exited = True

    def is_active(self) -> bool:
        return self._entered and not self._exited

    def register(self, lzy_op: LzyOp) -> None:
        self._ops.append(lzy_op)
        if self._eager:
            lzy_op.materialize()

    def registered_ops(self) -> Iterable[LzyOp]:
        if not self._entered:
            raise ValueError('Fetching ops on a non-entered environment')
        return list(self._ops)

    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        return self._wb_repo.whiteboards(typ)

    def projections(self, typ: Type[T]) -> Iterable[T]:
        wb_arg_name = None
        wb_arg_type = None
        for k, v in inspect.signature(typ).parameters.items():
            if dataclasses.is_dataclass(v.annotation):
                wb_arg_type = v.annotation
                wb_arg_name = k

        if wb_arg_type is None:
            raise ValueError('Projection class should accept whiteboard dataclass as an init argument')

        # noinspection PyArgumentList
        return map(lambda x: typ(**{wb_arg_name: x}), self._wb_repo.whiteboards(wb_arg_type))

    def run(self) -> None:
        if not self._entered:
            raise ValueError('Run operation on a non-entered environment')
        if self._exited:
            raise ValueError('Run operation on an exited environment')
        # noinspection PyTypeChecker
        if len(self._ops) == 0:
            raise ValueError('No registered ops')
        for wrapper in self._ops:
            wrapper.materialize()


class LzyUtils:
    @staticmethod
    def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
        for lzy_op in ops:
            inp = "("
            ret = ""
            hints = get_type_hints(lzy_op.func())
            keys = list(hints.keys())
            for i in range(len(keys)):
                if keys[i] != 'return':
                    inp += str(hints[keys[i]])
                    inp += ","
                else:
                    ret += str(hints[keys[i]])
            inp += ")"
            print(
                inp + " -> " + str(lzy_op.func()) + " -> " + ret + ", materialized=" + str(lzy_op.is_materialized()))
