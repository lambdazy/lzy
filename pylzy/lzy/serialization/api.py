import abc
from typing import BinaryIO, Type, Union, Callable, Any, Optional, TypeVar

T = TypeVar("T")


class Serializer(abc.ABC):
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        pass

    @abc.abstractmethod
    def deserialize(self, source: BinaryIO, typ: Type[T]) -> T:
        pass

    @abc.abstractmethod
    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        pass

    @abc.abstractmethod
    def available(self) -> bool:
        pass

    @abc.abstractmethod
    def stable(self) -> bool:
        pass


class SerializersRegistry(abc.ABC):
    @abc.abstractmethod
    def register_serializer(self, serializer: Serializer, priority: Optional[int] = None) -> None:
        pass

    @abc.abstractmethod
    def unregister_serializer(self, serializer: Serializer) -> None:
        pass

    @abc.abstractmethod
    def find_serializer_by_type(self, typ: Type) -> Serializer:
        pass

    @abc.abstractmethod
    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        pass


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        pass

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        pass
