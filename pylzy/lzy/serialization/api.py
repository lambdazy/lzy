from typing import Any, BinaryIO, Generic, Protocol, Type, TypeVar

T = TypeVar("T")  # pylint: disable=invalid-name


class Dumper(Protocol, Generic[T]):
    def dump(self, obj: T, dest: BinaryIO) -> None:
        pass

    def load(self, source: BinaryIO) -> T:
        pass

    def typ(self) -> Type[T]:
        pass

    def fit(self) -> bool:
        pass


class Serializer(Protocol):
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        pass

    def deserialize(self, src: BinaryIO, obj_type: Type[T] = None) -> T:
        pass

    def register_dumper(self, dump: Dumper) -> None:
        pass
