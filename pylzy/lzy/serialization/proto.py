from typing import BinaryIO, Type, Union, Callable

import cloudpickle
from pure_protobuf.dataclasses_ import Message  # type: ignore

from lzy.serialization.api import Serializer


# noinspection PyMethodMayBeStatic
class ProtoMessageSerializer(Serializer):
    def name(self) -> str:
        return "PROTO_MESSAGE_SERIALIZER"

    def serialize(self, obj: Message, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO) -> Message:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda t: issubclass(t, Message)
