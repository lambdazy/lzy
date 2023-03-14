import importlib
import copyreg
from dataclasses import dataclass
from io import BytesIO
from typing import Optional, Sequence, Type, Set, Any, cast

from serialzy.api import Serializer
from serialzy.registry import DefaultSerializerRegistry

from lzy.serialization.file import FileSerializer


@dataclass
class SerializerImport:
    module_name: str
    class_name: str
    priority: int


class LzySerializerRegistry(DefaultSerializerRegistry):
    def __init__(self):
        self.__inited = False
        self.__user_serializers: Set[Type[Serializer]] = set()

        super().__init__()

        self.register_serializer(FileSerializer())
        self.__inited = True

        for typ, serializers in self._type_registry.items():
            if any(s.available for s in serializers):
                copyreg.dispatch_table[typ] = self.__reducer

    def register_serializer(self, serializer: Serializer, priority: Optional[int] = None) -> None:
        if type(serializer).__module__ == "__main__":
            raise ValueError("Cannot register serializers from the __main__ module. "
                             "Please move serializer class to an external file.")
        super().register_serializer(serializer, priority)
        if self.__inited:
            self.__user_serializers.add(type(serializer))

        try:
            if isinstance(serializer.supported_types(), Type):  # type: ignore
                copyreg.dispatch_table[serializer.supported_types()] = self.__reducer
        except (ImportError, ModuleNotFoundError):
            pass

    def unregister_serializer(self, serializer: Serializer):
        super().unregister_serializer(serializer)
        typ = type(serializer)
        if typ in self.__user_serializers:
            self.__user_serializers.remove(typ)

        try:
            if isinstance(serializer.supported_types(), Type):  # type: ignore
                del copyreg.dispatch_table[serializer.supported_types()]
        except (ImportError, ModuleNotFoundError):
            pass

    def imports(self) -> Sequence[SerializerImport]:
        result = []
        for serializer in self.__user_serializers:
            result.append(SerializerImport(serializer.__module__, serializer.__name__,
                                           self._serializer_priorities[serializer]))
        return result

    def load_imports(self, imports: Sequence[SerializerImport]):
        for imp in imports:
            module = importlib.import_module(imp.module_name)
            serializer = getattr(module, imp.class_name)
            self.register_serializer(serializer(), imp.priority)

    def __constructor(self, data_format: str, data: bytes) -> Any:
        serializer = cast(Serializer, self.find_serializer_by_data_format(data_format))
        value = serializer.deserialize(BytesIO(data))
        return value

    def __reducer(self, obj: Any):
        serializer = cast(Serializer, self.find_serializer_by_instance(obj))
        buffer = BytesIO()
        serializer.serialize(obj, buffer)
        return self.__constructor, (serializer.data_format(), buffer.getvalue())
