import logging
from typing import Any, BinaryIO, Callable, Dict, Type, Union

from lzy.serialization.api import DefaultDataSchemaSerializer, StandardDataFormats, Schema
from lzy.serialization.utils import cached_installed_packages
from packaging import version

_LOG = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class ProtoMessageSerializer(DefaultDataSchemaSerializer):
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))
        self._lib_name = "pure-protobuf"
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
        return {self._lib_name: self._pure_proto_version}

    def resolve(self, schema: Schema) -> Type:
        typ = super().resolve(schema)
        if 'pure-protobuf' not in schema.meta:
            _LOG.warning('No pure-protobuf version in meta')
        elif version.parse(schema.meta['pure-protobuf']) > version.parse(cached_installed_packages["pure-protobuf"]):
            _LOG.warning(f'Installed version of pure-protobuf {cached_installed_packages["pure-protobuf"]} '
                         f'is older than used for serialization {schema.meta["pure-protobuf"]}')
        return typ
