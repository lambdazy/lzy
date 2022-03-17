from io import BytesIO

import cloudpickle
from lzy.api.utils import check_message_field
from pure_protobuf.dataclasses_ import loads, load  # type: ignore
from typing import Type, TypeVar, Union, BinaryIO

T = TypeVar("T")  # pylint: disable=invalid-name


class Serializer:
    @staticmethod
    def deserialize_from_byte_string(data: bytes, obj_type: Type[T] = None) -> T:
        if check_message_field(obj_type):
            return loads(obj_type, data)  # type: ignore
        return cloudpickle.loads(data)  # type: ignore

    @staticmethod
    def deserialize_from_file(data: Union[BinaryIO, BytesIO], obj_type: Type[T] = None) -> T:
        if check_message_field(obj_type):
            return load(obj_type, data)  # type: ignore
        return cloudpickle.load(data)  # type: ignore

    @staticmethod
    def serialize_to_file(obj: T, file: Union[BinaryIO, BytesIO], obj_type: Type[T] = None):
        if check_message_field(obj_type) or check_message_field(obj):
            obj.dump(file)  # type: ignore
        else:
            cloudpickle.dump(obj, file)

    @staticmethod
    def serialize_to_byte_string(obj: T, obj_type: Type[T] = None) -> bytes:
        if check_message_field(obj_type) or check_message_field(obj):
            return obj.dumps()  # type: ignore
        else:
            return cloudpickle.dumps(obj)
