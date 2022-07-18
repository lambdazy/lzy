from abc import ABC, abstractmethod
from typing import Any, IO, Type, TypeVar, Generic

T = TypeVar("T")  # pylint: disable=invalid-name


class FileSerializer(ABC):
    @abstractmethod
    def serialize_to_file(self, obj: Any, file: IO) -> None:
        pass

    @abstractmethod
    def deserialize_from_file(self, data: IO, obj_type: Type[T] = None) -> T:
        pass


class MemBytesSerializer(ABC):
    @abstractmethod
    def serialize_to_string(self, obj: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize_from_string(self, data: bytes, obj_type: Type[T] = None) -> T:
        pass


class Dumper(ABC, Generic[T]):
    @abstractmethod
    def dump(self, obj: T, dest: IO) -> None:
        pass

    @abstractmethod
    def load(self, source: IO) -> T:
        pass

    @abstractmethod
    def typ(self) -> Type[T]:
        pass

    @abstractmethod
    def fit(self) -> bool:
        pass