from typing import Any, BinaryIO, Callable, Dict, Type, Union, Optional

from packaging import version  # type: ignore
from serialzy.api import StandardDataFormats, VersionBoundary
from serialzy.base import DefaultSchemaSerializerByReference


# noinspection PyPackageRequirements
class TestStrSerializer(DefaultSchemaSerializerByReference):
    def _serialize(self, obj: Any, dest: BinaryIO) -> None:
        dumps = str(obj).encode("utf-8")
        dest.write(dumps)

    def _deserialize(self, source: BinaryIO, schema_type: Type, user_type: Optional[Type] = None) -> Any:
        return "DESERIALIZED"

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return str

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def data_format(self) -> str:
        return StandardDataFormats.primitive_type.name

    def meta(self) -> Dict[str, str]:
        return {}

    def requirements(self) -> Dict[str, VersionBoundary]:
        return {}
