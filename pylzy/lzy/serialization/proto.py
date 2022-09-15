import logging
from typing import Any, BinaryIO, Callable, Dict, Type, Union

from lzy.serialization.api import Serializer, StandardDataFormats
from lzy.serialization.utils import cached_installed_packages


# noinspection PyMethodMayBeStatic
class ProtoMessageSerializer(Serializer):
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))
        self._pure_proto_version = "unknown"

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

            # hidden-pure-protobuf is used until `oneof` functionality will be published in the main package
            if "hidden-pure-protobuf" in cached_installed_packages:
                self._pure_proto_version = cached_installed_packages[
                    "hidden-pure-protobuf"
                ]
            elif "pure-protobuf" in cached_installed_packages:
                self._pure_proto_version = cached_installed_packages["pure-protobuf"]
            return True
        except:
            logging.warning("Cannot import pure-protobuf")
            return False

    def stable(self) -> bool:
        return True

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        from pure_protobuf.dataclasses_ import Message  # type: ignore

        return lambda t: issubclass(t, Message)

    def format(self) -> str:
        return StandardDataFormats.proto.name

    def meta(self) -> Dict[str, str]:
        return {"pure_protobuf_version": self._pure_proto_version}
