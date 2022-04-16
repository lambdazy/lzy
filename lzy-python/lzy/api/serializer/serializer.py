from abc import abstractmethod
from io import BytesIO

import cloudpickle
from lzy.api.utils import check_message_field
from pure_protobuf.dataclasses_ import loads, load  # type: ignore
from typing import Type, TypeVar, Union, BinaryIO

T = TypeVar("T")  # pylint: disable=invalid-name


class FileSerializer:
    @abstractmethod
    def serialize(self, obj: T, file: Union[BinaryIO, BytesIO], obj_type: Type[T] = None) -> None:
        pass

    @abstractmethod
    def deserialize(self, data: Union[BinaryIO, BytesIO], obj_type: Type[T] = None) -> T:
        pass


class MemBytesSerializer:
    @abstractmethod
    def serialize(self, obj: T, obj_type: Type[T] = None) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes, obj_type: Type[T] = None) -> T:
        pass


class FileSerializerImpl(FileSerializer):
    def serialize(self, obj: T, file: Union[BinaryIO, BytesIO], obj_type: Type[T] = None) -> None:
        if check_message_field(obj_type) or check_message_field(obj):
            obj.dump(file)  # type: ignore
        else:
            cloudpickle.dump(obj, file)

    def deserialize(self, data: Union[BinaryIO, BytesIO], obj_type: Type[T] = None) -> T:
        if check_message_field(obj_type):
            return load(obj_type, data)  # type: ignore
        return cloudpickle.load(data)  # type: ignore


class MemBytesSerializerImpl(MemBytesSerializer):
    def serialize(self, obj: T, obj_type: Type[T] = None) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize(self, data: bytes, obj_type: Type[T] = None) -> T:
        return cloudpickle.load(data)  # type: ignore
