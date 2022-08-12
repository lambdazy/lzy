from typing import BinaryIO, Type, Union, Callable, Any

import cloudpickle
from pure_protobuf.dataclasses_ import Message  # type: ignore

from lzy.serialization.api import Serializer


# noinspection PyMethodMayBeStatic
class CloudpickleSerializer(Serializer):
    def name(self) -> str:
        return "CLOUDPICKLE_SERIALIZER"

    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO) -> Any:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return False

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: True
