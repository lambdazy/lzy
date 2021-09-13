from abc import abstractmethod
from typing import Any, Iterable, Type, Callable, TypeVar, Generic, Optional, List, Set

from _collections import defaultdict

from lzy.op import LzyOp
from lzy.proxy import Proxy

T = TypeVar('T')


class WbFieldProxy(Proxy):
    def __init__(self, origin: Any, deps: Set[str]):
        super().__init__(type(origin))
        self._origin = origin
        self._deps = set(deps)

    def on_call(self, name: str, *args) -> Any:
        return getattr(self._origin, name)(*args)

    def deps(self):
        return set(self._deps)


class WhiteboardsRepo:
    @abstractmethod
    def register(self, wb: Any) -> None:
        pass

    @abstractmethod
    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        pass


class WhiteboardsRepoInMem(WhiteboardsRepo):
    def __init__(self):
        super().__init__()
        self._whiteboards = defaultdict(list)

    def register(self, wb: Any) -> None:
        self._whiteboards[type(wb)].append(wb)

    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        return self._whiteboards[typ]


class WhiteboardController(Generic[T]):
    @abstractmethod
    def capture(self) -> None:
        pass

    @abstractmethod
    def finalize(self) -> T:
        pass


class WhiteboardControllerImpl(WhiteboardController):
    def __init__(self, whiteboard: T):
        super().__init__()
        self._whiteboard = whiteboard
        self._already_set_fields = set()
        self._dependencies = defaultdict(set)

    def capture(self) -> None:
        set_attr = getattr(self._whiteboard, '__setattr__')
        setattr(self._whiteboard, '__setattr__', lambda *a: self._fake_setattr(set_attr, *a))
        setattr(type(self._whiteboard), '__setattr__', lambda obj, *a: obj.__setattr__(*a))

    def finalize(self) -> T:
        self._already_set_fields.clear()
        for k, v in self._whiteboard.__dict__.items():
            if isinstance(v, LzyOp):
                self._compute_dependencies([k], v)
                setattr(self._whiteboard, k, WbFieldProxy(v.materialize(), self._dependencies[k]))
        setattr(self._whiteboard, '__setattr__', lambda *a: self._raise_write_exception())
        setattr(type(self._whiteboard), '__setattr__', lambda obj, *a: obj.__setattr__(*a))
        return self._whiteboard

    def _compute_dependencies(self, roots: List[str], op: LzyOp) -> None:
        for arg in op.args():
            field = self._find_wb_field(arg)
            roots_copy = list(roots)
            if field is not None:
                for root in roots:
                    if field in self._dependencies[root]:
                        continue
                    else:
                        self._dependencies[root].add(field)
                roots_copy.append(field)
            if isinstance(arg, LzyOp):
                self._compute_dependencies(roots_copy, arg)

    def _find_wb_field(self, o: Any) -> Optional[str]:
        for k, v in self._whiteboard.__dict__.items():
            # noinspection PyTypeChecker
            if id(v) == id(o):
                return k
        return None

    def _fake_setattr(self, set_attr: Callable, name: str, value: Any) -> None:
        if name in self._already_set_fields:
            raise ValueError('Item has been already set to the whiteboard')
        self._already_set_fields.add(name)
        set_attr(name, value)

    def _raise_write_exception(self) -> None:
        raise ValueError('Writes to a whiteboard are forbidden after run')
