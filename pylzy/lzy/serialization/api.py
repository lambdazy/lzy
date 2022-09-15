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
        """
        :param obj: object to serialize into bytes
        :param dest: serialized obj is written into dest
        :return: None
        """

    @abc.abstractmethod
    def deserialize(self, source: BinaryIO, typ: Type[T]) -> T:
        """
        :param source: buffer of file with serialized data
        :param typ: type of the resulting object
        :return: deserialized object
        """

    @abc.abstractmethod
    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        """
        :return: type suitable for the serializer or types filter
        """

    @abc.abstractmethod
    def available(self) -> bool:
        """
        :return: True if the serializer can be used in the current environment, otherwise False
        """

    @abc.abstractmethod
    def stable(self) -> bool:
        """
        :return: True if the serializer does not depend on python version/dependency versions/etc., otherwise False
        """

    @abc.abstractmethod
    def format(self) -> str:
        """
        :return: data format that this serializer is working with
        """

    @abc.abstractmethod
    def meta(self) -> Dict[str, str]:
        """
        :return: meta of this serializer, e.g., versions of dependencies
        """

    def schema(self, obj: Any) -> Schema:
        """
        :param obj: object for serialization
        :return: schema for the object
        """
        return Schema(
            self.format(),
            StandardSchemaFormats.pickled_type.name,
            base64.b64encode(cloudpickle.dumps(type(obj))).decode("ascii"),
            self.meta(),
        )

    # noinspection PyMethodMayBeStatic
    def resolve(self, schema: Schema) -> Type:
        """
        :param schema: schema that contains information about serialized data
        :return: Type used for python representation of the schema
        """
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
        """
        :param name: unique serializer's name
        :param serializer: serializer to register
        :param priority: number that indicates serializer's priority: 0 - max priority
        :return: None
        """

    @abc.abstractmethod
    def unregister_serializer(self, name: str) -> None:
        """
        :param name: name of the serializer to unregister
        :return:
        """

    @abc.abstractmethod
    def find_serializer_by_type(
        self, typ: Type
    ) -> Serializer:  # we assume that default serializer always can be found
        """
        :param typ: python Type needed to serialize
        :return: corresponding serializer
        """

    @abc.abstractmethod
    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        """
        :param serializer_name: target name
        :return: Serializer registered with serializer_name or None
        """

    @abc.abstractmethod
    def resolve_name(self, serializer: Serializer) -> Optional[str]:
        """
        :param serializer: serializer to resolve name
        :return: name if the serializer is registered, None otherwise
        """

    @abc.abstractmethod
    def find_serializer_by_data_format(self, data_format: str) -> Optional[Serializer]:
        """
        :param data_format: data format to resolve serializer
        :return: Serializer if there is a serializer for that data format, None otherwise
        """


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        """
        :param data: object to hash
        :return: hash result
        """

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        """
        :param data: object to hash
        :return: True if object can be hashed, False otherwise
        """
