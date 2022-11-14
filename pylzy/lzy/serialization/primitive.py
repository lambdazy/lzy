import logging
from typing import Any, BinaryIO, Callable, Dict, Type, Union, cast
from packaging import version

import jsonpickle  # type: ignore

from lzy.serialization.api import (
    Schema,
    Serializer,
    StandardDataFormats,
    StandardSchemaFormats,
    T,
)
from lzy.serialization.utils import cached_installed_packages

_LOG = logging.getLogger(__name__)


class PrimitiveSerializer(Serializer):
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        dumps = jsonpickle.dumps(obj).encode("utf-8")
        dest.write(dumps)

    def deserialize(self, source: BinaryIO, typ: Type[T]) -> T:
        read = source.read().decode("utf-8")
        return cast(T, jsonpickle.loads(read))

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda t: t in [int, float, str, bool]

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def format(self) -> str:
        return StandardDataFormats.primitive_type.name

    def meta(self) -> Dict[str, str]:
        return {"jsonpickle": jsonpickle.__version__}

    def schema(self, typ: type) -> Schema:
        return Schema(
            self.format(),
            StandardSchemaFormats.json_pickled_type.name,
            jsonpickle.dumps(typ),
            self.meta(),
        )

    def resolve(self, schema: Schema) -> Type:
        self._validate_schema(schema)
        if schema.schema_format != StandardSchemaFormats.json_pickled_type.name:
            raise ValueError('PrimitiveSerializer supports only jsonpickle schema format')
        if 'jsonpickle' not in schema.meta:
            _LOG.warning('No jsonpickle version in meta')
        elif version.parse(schema.meta['jsonpickle']) > version.parse(cached_installed_packages["jsonpickle"]):
            _LOG.warning(f'Installed version of jsonpickle {cached_installed_packages["jsonpickle"]} '
                         f'is older than used for serialization {schema.meta["jsonpickle"]}')
        return cast(Type, jsonpickle.loads(schema.schema_content))
