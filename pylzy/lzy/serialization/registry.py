import logging
import sys
from typing import Dict, List, Type, Optional, cast

from lzy.serialization.api import SerializersRegistry, Serializer
from lzy.serialization.catboost import CatboostPoolSerializer
from lzy.serialization.file import FileSerializer
from lzy.serialization.proto import ProtoMessageSerializer
from lzy.serialization.universal import CloudpickleSerializer


class DefaultSerializersRegistry(SerializersRegistry):
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))

        self._default_priority = sys.maxsize - 10
        self._type_registry: Dict[Type, Serializer] = {}
        self._name_registry: Dict[str, Serializer] = {}
        self._filters_registry: List[Serializer] = []
        self._serializer_priorities: Dict[str, int] = {}

        self.register_serializer(CatboostPoolSerializer(), self._default_priority)
        self.register_serializer(FileSerializer(), self._default_priority)

        self.register_serializer(ProtoMessageSerializer(), self._default_priority)
        self.register_serializer(CloudpickleSerializer(), sys.maxsize - 1)

    def register_serializer(self, serializer: Serializer, priority: Optional[int] = None) -> None:
        if not serializer.available():
            self._log.warning(f"Serializer {serializer.name()} cannot be imported")
            return

        if serializer.name() in self._serializer_priorities:
            self._log.warning(f"Serializer {serializer.name()} has been already imported")
            return

        priority = self._default_priority if priority is None else priority
        self._serializer_priorities[serializer.name()] = priority
        self._name_registry[serializer.name()] = serializer
        # mypy issue: https://github.com/python/mypy/issues/3060
        if isinstance(serializer.supported_types(), Type):  # type: ignore
            self._type_registry[cast(Type, serializer.supported_types())] = serializer
        else:
            self._filters_registry.append(serializer)

    def unregister_serializer(self, serializer: Serializer):
        if serializer.name() in self._serializer_priorities:
            del self._serializer_priorities[serializer.name()]
            del self._name_registry[serializer.name()]
            # mypy issue: https://github.com/python/mypy/issues/3060
            if isinstance(serializer.supported_types(), Type):  # type: ignore
                del self._type_registry[cast(Type, serializer.supported_types())]
            else:
                self._filters_registry[:] = [x for x in self._filters_registry if x.name() != serializer.name()]

    def find_serializer_by_type(self, typ: Type) -> Serializer:
        filter_ser: Optional[Serializer] = None
        filter_ser_priority = sys.maxsize
        for serializer in self._filters_registry:
            if serializer.supported_types()(typ) and \
                    self._serializer_priorities[serializer.name()] < filter_ser_priority:
                filter_ser_priority = self._serializer_priorities[serializer.name()]
                filter_ser = serializer

        obj_type_ser: Optional[Serializer] = self._type_registry[typ] if typ in self._type_registry else None
        obj_type_ser_priority = sys.maxsize if obj_type_ser is None else self._serializer_priorities[
            obj_type_ser.name()]

        if filter_ser is not None and obj_type_ser is not None:
            return filter_ser if filter_ser_priority < obj_type_ser_priority else obj_type_ser
        elif filter_ser is not None:
            return filter_ser
        elif obj_type_ser is not None:
            return obj_type_ser
        else:
            raise ValueError(f"Could not find serializer for type {typ}")

    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        if serializer_name in self._name_registry:
            return self._name_registry[serializer_name]
        return None
