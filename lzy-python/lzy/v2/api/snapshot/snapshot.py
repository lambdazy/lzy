from abc import ABC, abstractmethod
from typing import Type, TypeVar, Any

T = TypeVar("T")  # pylint: disable=invalid-name


class Snapshot(ABC):
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def shape(self, wb_type: Type[T]) -> T:
        pass

    @abstractmethod
    def get(self, entry_id: str) -> Any:
        pass

    @abstractmethod
    def silent(self) -> None:
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractmethod
    def error(self):
        pass
