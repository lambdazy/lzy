import importlib
from dataclasses import dataclass
from typing import Optional, List, Sequence

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
        self.__user_serializers: List[Serializer] = []
        super().__init__()
        self.register_serializer(FileSerializer())
        self.__inited = True

    def register_serializer(self, serializer: Serializer, priority: Optional[int] = None) -> None:
        if type(serializer).__module__ == "__main__":
            raise ValueError("Cannot register serializers from the __main__ module. "
                             "Please move serializer class to an external file.")
        super().register_serializer(serializer, priority)
        if self.__inited:
            self.__user_serializers.append(serializer)

    def imports(self) -> Sequence[SerializerImport]:
        result = []
        for serializer in self.__user_serializers:
            typ = type(serializer)
            result.append(SerializerImport(typ.__module__, typ.__name__,
                                           self._serializer_priorities[type(serializer)]))
        return result

    def load_imports(self, imports: Sequence[SerializerImport]):
        for imp in imports:
            module = importlib.import_module(imp.module_name)
            serializer = getattr(module, imp.class_name)
            self.register_serializer(serializer(), imp.priority)
