from typing import Any, BinaryIO, Callable, Dict, Type, Union, cast

import jsonpickle  # type: ignore

from lzy.serialization.api import (
    Schema,
    Serializer,
    StandardDataFormats,
    StandardSchemaFormats,
    T,
)


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

    def schema_format(self) -> str:
        return StandardSchemaFormats.json_pickled_type.name

    def schema(self, typ: type) -> Schema:
        return Schema(
            self.format(),
            self.schema_format(),
            jsonpickle.dumps(typ),
            self.meta(),
        )

    def resolve(self, schema: Schema) -> Type:
        self._fail_if_formats_are_invalid(schema)
        self._fail_if_schema_content_none(schema)
        self._warn_if_metas_are_not_equal(schema)
        return cast(Type, jsonpickle.loads(schema.schema_content))
