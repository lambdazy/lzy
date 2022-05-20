from abc import abstractmethod, ABC
from typing import Type, TypeVar, IO, Any, Dict

import cloudpickle # type: ignore
from pure_protobuf.dataclasses_ import loads, load  # type: ignore
from lzy.api.v2.utils import check_message_field
from lzy.serialization.dumper import Dumper, CatboostPoolDumper

T = TypeVar("T")  # pylint: disable=invalid-name


class FileSerializer(ABC):
    @abstractmethod
    def serialize_to_file(self, obj: Any, file: IO) -> None:
        pass

    @abstractmethod
    def deserialize_from_file(self, data: IO, obj_type: Type[T] = None) -> T:
        pass


class MemBytesSerializer(ABC):
    @abstractmethod
    def serialize_to_string(self, obj: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize_from_string(self, data: bytes, obj_type: Type[T] = None) -> T:
        pass


class FileSerializerImpl(FileSerializer):
    def __init__(self):
        self._registry: Dict[Type, Dumper] = {}
        dumpers = [CatboostPoolDumper()]
        for dumper in dumpers:
            if dumper.fit():
                self._registry[dumper.typ()] = dumper

    def serialize_to_file(self, obj: Any, file: IO) -> None:
        if type(obj) in self._registry:
            dumper = self._registry[type(obj)]
            dumper.dump(obj, file)
        elif check_message_field(type(obj)) or check_message_field(obj):
            obj.dump(file)  # type: ignore
        else:
            cloudpickle.dump(obj, file)

    def deserialize_from_file(self, data: IO, obj_type: Type[T] = None) -> T:
        if obj_type in self._registry:
            dumper = self._registry[obj_type]
            return dumper.load(data)
        elif check_message_field(obj_type):
            return load(obj_type, data)  # type: ignore
        return cloudpickle.load(data)  # type: ignore


class MemBytesSerializerImpl(MemBytesSerializer):
    def serialize_to_string(self, obj: Any) -> bytes:
        # TODO: check for proto fields?
        return cloudpickle.dumps(obj)  # type: ignore

    def deserialize_from_string(self, data: bytes, obj_type: Type[T] = None) -> T:
        # TODO: check for proto fields?
        return cloudpickle.loads(data)  # type: ignore


class Serializer(FileSerializer, MemBytesSerializer):
    """serialization facility"""

    @abstractmethod
    def add_dumper(self, dumper: Dumper):
        pass


class DefaultSerializer(Serializer):
    def __init__(self):
        self._file_serializer: FileSerializer = FileSerializerImpl()
        self._mem_bytes_serializer: MemBytesSerializer = MemBytesSerializerImpl()

    def serialize_to_file(self, obj: Any, file: IO) -> None:
        self._file_serializer.serialize_to_file(obj, file)

    def deserialize_from_file(self, data: IO, obj_type: Type[T] = None) -> T:
        return self._file_serializer.deserialize_from_file(data, obj_type)

    def serialize_to_string(self, obj: Any) -> bytes:
        return self._mem_bytes_serializer.serialize_to_string(obj)

    def deserialize_from_string(self, data: bytes, obj_type: Type[T] = None) -> T:
        return self._mem_bytes_serializer.deserialize_from_string(data, obj_type)

    def add_dumper(self, dumper: Dumper):
        # TODO
        pass
