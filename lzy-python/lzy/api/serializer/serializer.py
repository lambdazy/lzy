from abc import ABC
import cloudpickle
from lzy.api.whiteboard import check_message_field
from pure_protobuf.dataclasses_ import loads, load  # type: ignore
from typing import Type, TypeVar

T = TypeVar("T")  # pylint: disable=invalid-name


class Serializer(ABC):
    def deserialize_from_byte_string(self, data, obj_type: Type[T] = None):
        if check_message_field(obj_type):
            return loads(obj_type, data)
        return cloudpickle.loads(data)

    def deserialize_from_file(self, data, obj_type: Type[T] = None):
        if check_message_field(obj_type):
            return load(obj_type, data)
        return cloudpickle.load(data)

    def serialize_to_file(self, obj, file, obj_type: Type[T] = None):
        if check_message_field(obj_type) or check_message_field(obj):
            obj.dump(file)
        else:
            cloudpickle.dump(obj, file)

    def serialize_to_byte_string(self, obj, obj_type: Type[T] = None):
        if check_message_field(obj_type) or check_message_field(obj):
            return obj.dumps()
        else:
            return cloudpickle.dumps(obj)
