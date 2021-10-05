from abc import abstractmethod, ABC
from collections import defaultdict
from typing import Any, TypeVar, Type, Iterable

T = TypeVar('T')


class WhiteboardsRepo(ABC):
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
