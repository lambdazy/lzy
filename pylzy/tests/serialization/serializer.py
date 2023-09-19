from typing import Any, BinaryIO, Callable, Dict, Type, Union, Optional

from serialzy.api import VersionBoundary
from serialzy.base import DefaultSchemaSerializerByReference


# noinspection PyPackageRequirements
class TestSerializer(DefaultSchemaSerializerByReference):
    def _serialize(self, obj: Any, dest: BinaryIO) -> None:
        pass

    def _deserialize(self, source: BinaryIO, schema_type: Type, user_type: Optional[Type] = None) -> Any:
        return None

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: True

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def data_format(self) -> str:
        return "test_format"

    def meta(self) -> Dict[str, str]:
        return {}

    def requirements(self) -> Dict[str, VersionBoundary]:
        return {}
