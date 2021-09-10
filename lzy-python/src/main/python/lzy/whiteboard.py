from typing import Any, Iterable, Type, Callable, TypeVar, Generic

from _collections import defaultdict

T = TypeVar('T')


class WhiteboardsRepo:
    def __init__(self):
        self._whiteboards = defaultdict(list)

    def register(self, wb: Any) -> None:
        self._whiteboards[type(wb)].append(wb)

    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        return self._whiteboards[typ]


class WhiteboardProxy(Generic[T]):
    def __init__(self, whiteboard: T):
        super().__init__()
        self._whiteboard = whiteboard
        self._already_set_fields = set()

    def disallow_multiple_writes(self) -> None:
        if self._whiteboard is not None:
            set_attr = getattr(self._whiteboard, '__setattr__')
            setattr(self._whiteboard, '__setattr__', lambda *a: self._fake_setattr(set_attr, *a))
            setattr(type(self._whiteboard), '__setattr__', lambda obj, *a: obj.__setattr__(*a))

    def disallow_writes(self) -> None:
        if self._whiteboard is not None:
            setattr(self._whiteboard, '__setattr__', lambda *a: self._raise_write_exception())
            setattr(type(self._whiteboard), '__setattr__', lambda obj, *a: obj.__setattr__(*a))

    def whiteboard(self) -> T:
        return self._whiteboard

    def _fake_setattr(self, set_attr: Callable, name: str, value: Any) -> None:
        if name in self._already_set_fields:
            raise ValueError('Item has been already set to the whiteboard')
        self._already_set_fields.add(name)
        set_attr(name, value)

    def _raise_write_exception(self) -> None:
        raise ValueError('Writes to a whiteboard are forbidden after run')
