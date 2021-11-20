from abc import abstractmethod
from typing import Dict, Generic, Set, TypeVar, Any

from collections import defaultdict

T = TypeVar("T")


class WhiteboardController(Generic[T]):
    @abstractmethod
    def capture(self) -> None:
        pass

    @abstractmethod
    def finalize(self) -> T:
        pass


class WhiteboardControllerImpl(WhiteboardController, Generic[T]):
    def __init__(self, whiteboard: T):
        super().__init__()
        self._whiteboard = whiteboard
        self._already_set_fields: Set[str] = set()
        self._dependencies: Dict[str, Set[Any]] = defaultdict(set)

    def finalize(self) -> T:
        return self._whiteboard
    
    def capture(self) -> None:
        # TODO implement this
        pass