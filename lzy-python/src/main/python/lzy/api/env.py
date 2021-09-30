from abc import abstractmethod, ABC
import dataclasses
import inspect
import logging
from typing import List, Tuple, Callable, Type, Any, TypeVar, Iterable

from .buses import Bus
from .lazy_op import LzyOp
from whiteboard import WhiteboardsRepoInMem, WhiteboardControllerImpl

T = TypeVar('T')


class LzyEnvBase(ABC):
    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def is_local(self) -> bool:
        pass

    @abstractmethod
    def register_op(self, lzy_op: LzyOp) -> None:
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
    instance = None

    # noinspection PyDefaultArgument
    def __init__(self, eager: bool = False, whiteboard: Any = None,
                 buses: List[Tuple[Callable, Bus]] = [], local: bool = False):
        super().__init__()
        # if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
        #     raise ValueError('Whiteboard should be a dataclass')
        if whiteboard is not None:
            self._wb_controller = WhiteboardControllerImpl(whiteboard)
        else:
            self._wb_controller = None

        self._wb_repo = WhiteboardsRepoInMem()
        self._ops = []
        self._eager = eager
        self._local = local
        self._buses = list(buses)
        self._log = logging.getLogger(str(self.__class__))

    # TODO: mb better naming
    def already_exists(self):
        cls = type(self)
        return hasattr(cls, 'instance') and cls.instance is not None

    def activate(self):
        type(self).instance = self

    def deactivate(self):
        type(self).instance = None

    def __enter__(self):  # -> LzyEnv
        if self.already_exists():
            raise ValueError('More than one started lzy environment found')
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.run()
            if self._wb_controller is not None:
                self._wb_repo.register(self._wb_controller)
        finally:
            self.deactivate()

    def is_active(self) -> bool:
        return self.already_exists() and type(self).instance is self

    def is_local(self) -> bool:
        return self._local

    def register_op(self, lzy_op: LzyOp) -> None:
        self._ops.append(lzy_op)
        if self._eager:
            lzy_op.materialize()

    def registered_ops(self) -> Iterable[LzyOp]:
        if not self.already_exists():
            raise ValueError('Fetching ops on a non-entered environment')
        return list(self._ops)

    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        return self._wb_repo.whiteboards(typ)

    def projections(self, typ: Type[T]) -> Iterable[T]:
        # TODO: UPDATE with new WB
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
        if not self.already_exists():
            raise ValueError('Run operation on a non-entered environment')
        if len(self._ops) == 0:
            raise ValueError('No registered ops')
        for wrapper in self._ops:
            wrapper.materialize()

    @classmethod
    def get_active(cls) -> 'LzyEnv':
        return cls.instance
