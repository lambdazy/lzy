from typing import Any, BinaryIO, Protocol, Type, TypeVar

T = TypeVar("T")  # pylint: disable=invalid-name


class Dumper(Protocol[T]):
    def dump(self, obj: T, dest: BinaryIO) -> None:
        pass

    def load(self, source: BinaryIO) -> T:
        pass

    def typ(self) -> Type[T]:
        pass

    def fit(self) -> bool:
        pass


class Serializer(Protocol):
    def serialize(self, obj: Any, file: BinaryIO) -> None:
        pass

    def deserialize(self, data: BinaryIO, obj_type: Type[T] = None) -> T:
        pass

    def add_dumper(self, dumper: Dumper):
        pass