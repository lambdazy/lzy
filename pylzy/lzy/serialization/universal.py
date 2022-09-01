from typing import Any, BinaryIO, Callable, Type, Union

import cloudpickle

from lzy.serialization.api import Serializer


# noinspection PyMethodMayBeStatic
class CloudpickleSerializer(Serializer):
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO, typ: Type) -> Any:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return False

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: True
