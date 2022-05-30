from abc import ABC, abstractmethod
from typing import Type, TypeVar, Any, Optional

from lzy.serialization.serializer import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


class Snapshot(ABC):
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def serializer(self) -> Serializer:
        pass

    @abstractmethod
    def shape(self, wb_type: Type[T]) -> T:
        pass

    @abstractmethod
    def get(self, entry_id: str) -> Optional[Any]:
        pass

    @abstractmethod
    def put(self, entry_id: str, data: Any) -> None:
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractmethod
    def error(self):
        pass
