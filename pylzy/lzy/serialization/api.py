import abc
from typing import Any, BinaryIO, Callable, Optional, Type, TypeVar, Union

T = TypeVar("T")


class Serializer(abc.ABC):
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
    def register_serializer(
        self, name: str, serializer: Serializer, priority: Optional[int] = None
    ) -> None:
        pass

    @abc.abstractmethod
    def unregister_serializer(self, name: str) -> None:
        pass

    @abc.abstractmethod
    def find_serializer_by_type(
        self, typ: Type
    ) -> Serializer:  # we assume that default serializer always can be found
        pass

    @abc.abstractmethod
    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        pass

    @abc.abstractmethod
    def resolve_name(self, serializer: Serializer) -> Optional[str]:
        pass


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        pass

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        pass
