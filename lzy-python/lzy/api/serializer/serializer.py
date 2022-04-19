from abc import abstractmethod
from typing import Type, TypeVar, IO, Any

import cloudpickle
from pure_protobuf.dataclasses_ import loads, load  # type: ignore

from lzy.api.utils import check_message_field

T = TypeVar("T")  # pylint: disable=invalid-name


class FileSerializer:
    @abstractmethod
    def serialize(self, obj: T, file: IO) -> None:
        pass

    @abstractmethod
    def deserialize(self, data: IO, obj_type: Type[T] = None) -> T:
        pass


class MemBytesSerializer:
    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes, obj_type: Type[T] = None) -> T:
        pass


class FileSerializerImpl(FileSerializer):
    def serialize(self, obj: T, file: IO) -> None:
        if check_message_field(type(obj)) or check_message_field(obj):
            obj.dump(file)  # type: ignore
        else:
            cloudpickle.dump(obj, file)

    def deserialize(self, data: IO, obj_type: Type[T] = None) -> T:
        if check_message_field(obj_type):
            return load(obj_type, data)  # type: ignore
        return cloudpickle.load(data)  # type: ignore


class MemBytesSerializerImpl(MemBytesSerializer):
    def serialize(self, obj: Any) -> bytes:
        return cloudpickle.dumps(obj)  # type: ignore

    def deserialize(self, data: bytes, obj_type: Type[T] = None) -> T:
        return cloudpickle.loads(data)  # type: ignore
