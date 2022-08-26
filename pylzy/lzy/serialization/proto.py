import logging
from typing import Any, BinaryIO, Callable, Type, Union

from lzy.serialization.api import Serializer


# noinspection PyMethodMayBeStatic
class ProtoMessageSerializer(Serializer):
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))

    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        obj.dump(dest)

    def deserialize(self, source: BinaryIO, typ: Type) -> Any:
        from pure_protobuf.dataclasses_ import load  # type: ignore

        # noinspection PyTypeChecker
        return load(typ, source)

    def available(self) -> bool:
        # noinspection PyBroadException
        try:
            import pure_protobuf  # type: ignore

            return True
        except:
            logging.warning("Cannot import pure-protobuf")
            return False

    def stable(self) -> bool:
        return True

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        from pure_protobuf.dataclasses_ import Message  # type: ignore

        return lambda t: issubclass(t, Message)
