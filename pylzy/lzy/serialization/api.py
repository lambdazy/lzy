import abc
import base64
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, BinaryIO, Callable, Dict, Optional, Type, TypeVar, Union, cast

import cloudpickle

T = TypeVar("T")


class StandardDataFormats(Enum):
    pickle = "pickle"
    proto = "proto"
    raw_file = "raw_file"
    primitive_type = "primitive_type"


class StandardSchemaFormats(Enum):
    pickled_type = "pickled_type"
    json_pickled_type = "json_pickled_type"


@dataclass
class Schema:
    data_format: str
    schema_format: str
    schema_content: Optional[str] = None
    meta: Dict[str, str] = field(default_factory=lambda: {})


class Serializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        """ abstract method """

    @abc.abstractmethod
    def deserialize(self, source: BinaryIO, typ: Type[T]) -> T:
        """ abstract method """

    @abc.abstractmethod
    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        """ abstract method """

    @abc.abstractmethod
    def available(self) -> bool:
        """ abstract method """

    @abc.abstractmethod
    def stable(self) -> bool:
        """ abstract method """

    @abc.abstractmethod
    def format(self) -> str:
        """ abstract method """

    @abc.abstractmethod
    def meta(self) -> Dict[str, str]:
        """ abstract method """

    def schema(self, obj: Any) -> Schema:
        return Schema(
            self.format(),
            StandardSchemaFormats.pickled_type.name,
            base64.b64encode(cloudpickle.dumps(type(obj))).decode("ascii"),
            self.meta(),
        )

    # noinspection PyMethodMayBeStatic
    def resolve(self, schema: Schema) -> Type:
        if schema.data_format != self.format():
            raise ValueError(
                f"Invalid data format {schema.data_format}, expected {self.format()}"
            )
        if schema.schema_format != StandardSchemaFormats.pickled_type.name:
            raise ValueError(f"Invalid schema format {schema.schema_format}")
        if schema.schema_content is None:
            raise ValueError(f"No schema content")
        return cast(
            Type,
            cloudpickle.loads(base64.b64decode(schema.schema_content.encode("ascii"))),
        )


class SerializerRegistry(abc.ABC):
    @abc.abstractmethod
    def register_serializer(
            self, name: str, serializer: Serializer, priority: Optional[int] = None
    ) -> None:
        """ abstract method """

    @abc.abstractmethod
    def unregister_serializer(self, name: str) -> None:
        """ abstract method """

    @abc.abstractmethod
    def find_serializer_by_type(
            self, typ: Type
    ) -> Serializer:  # we assume that default serializer always can be found
        """ abstract method """

    @abc.abstractmethod
    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        """ abstract method """

    @abc.abstractmethod
    def resolve_name(self, serializer: Serializer) -> Optional[str]:
        """ abstract method """

    @abc.abstractmethod
    def find_serializer_by_data_format(self, data_format: str) -> Optional[Serializer]:
        """ abstract method """


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        """ abstract method """

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        """ abstract method """
