from abc import abstractmethod
from typing import Generic, TypeVar

from collections import defaultdict

T = TypeVar("T")


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

    def finalize(self) -> T:
        return self._whiteboard
