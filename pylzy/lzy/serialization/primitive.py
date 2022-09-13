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
        return {"jsonpickle_version": jsonpickle.__version__}

    def schema(self, obj: Any) -> Schema:
        return Schema(
            self.format(),
            StandardSchemaFormats.json_pickled_type.name,
            jsonpickle.dumps(type(obj)),
            self.meta(),
        )

    def resolve(self, schema: Schema) -> Type:
        if schema.data_format != StandardDataFormats.primitive_type.name:
            raise ValueError(
                f"Invalid data format {schema.data_format}, expected {self.format()}"
            )
        if schema.schema_format != StandardSchemaFormats.json_pickled_type.name:
            raise ValueError(
                f"Invalid schema format {schema.schema_format}, expected {StandardSchemaFormats.json_pickled_type.name}"
            )
        return cast(Type, jsonpickle.loads(schema.schema_content))
