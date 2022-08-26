import logging
import sys
from collections import OrderedDict
from typing import Dict, Optional, Type, cast

from lzy.serialization.api import Serializer, SerializersRegistry
from lzy.serialization.catboost import CatboostPoolSerializer
from lzy.serialization.file import FileSerializer
from lzy.serialization.proto import ProtoMessageSerializer
from lzy.serialization.universal import CloudpickleSerializer


class DefaultSerializersRegistry(SerializersRegistry):
    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))

        self._default_priority = sys.maxsize - 10
        self._type_registry: Dict[Type, Serializer] = {}
        self._type_name_registry: Dict[Type, str] = {}
        self._name_registry: Dict[str, Serializer] = OrderedDict()
        self._serializer_priorities: Dict[str, int] = {}

        self.register_serializer(
            "LZY_CATBOOST_POOL_SERIALIZER",
            CatboostPoolSerializer(),
            self._default_priority,
        )
        self.register_serializer(
            "LZY_FILE_SERIALIZER", FileSerializer(), self._default_priority
        )

        self.register_serializer(
            "LZY_PROTO_MESSAGE_SERIALIZER",
            ProtoMessageSerializer(),
            self._default_priority,
        )
        self.register_serializer(
            "LZY_CLOUDPICKLE_SERIALIZER", CloudpickleSerializer(), sys.maxsize - 1
        )

    def register_serializer(
        self, name: str, serializer: Serializer, priority: Optional[int] = None
    ) -> None:
        if not serializer.available():
            self._log.warning(f"Serializer {name} cannot be registered")
            return

        if name in self._serializer_priorities:
            raise ValueError(f"Serializer {name} has been already registered")

        if isinstance(serializer.supported_types(), Type) and serializer.supported_types() in self._type_registry:  # type: ignore
            raise ValueError(
                f"Serializer for type {serializer.supported_types()} has been already registered"
            )

        priority = self._default_priority if priority is None else priority
        self._serializer_priorities[name] = priority
        self._name_registry[name] = serializer
        # mypy issue: https://github.com/python/mypy/issues/3060
        if isinstance(serializer.supported_types(), Type):  # type: ignore
            self._type_registry[cast(Type, serializer.supported_types())] = serializer
            self._type_name_registry[cast(Type, serializer.supported_types())] = name

    def unregister_serializer(self, name: str):
        if name in self._serializer_priorities:
            serializer = self._name_registry[name]
            # mypy issue: https://github.com/python/mypy/issues/3060
            if isinstance(serializer.supported_types(), Type):  # type: ignore
                del self._type_registry[cast(Type, serializer.supported_types())]
                del self._type_name_registry[cast(Type, serializer.supported_types())]
            del self._serializer_priorities[name]
            del self._name_registry[name]

    def find_serializer_by_type(self, typ: Type) -> Serializer:
        filter_ser: Optional[Serializer] = None
        filter_ser_priority = sys.maxsize
        for name, serializer in self._name_registry.items():
            if (
                # mypy issue: https://github.com/python/mypy/issues/3060
                not isinstance(serializer.supported_types(), Type)  # type: ignore
                and serializer.supported_types()(typ)
                and self._serializer_priorities[name] < filter_ser_priority
            ):
                filter_ser_priority = self._serializer_priorities[name]
                filter_ser = serializer

        obj_type_ser: Optional[Serializer] = (
            self._type_registry[typ] if typ in self._type_registry else None
        )
        obj_type_ser_priority = (
            sys.maxsize
            if obj_type_ser is None
            else self._serializer_priorities[self._type_name_registry[typ]]
        )

        if obj_type_ser is not None:
            return (
                cast(Serializer, filter_ser)
                if filter_ser_priority < obj_type_ser_priority
                else obj_type_ser
            )
        return cast(Serializer, filter_ser)

    def find_serializer_by_name(self, serializer_name: str) -> Optional[Serializer]:
        if serializer_name in self._name_registry:
            return self._name_registry[serializer_name]
        return None

    def resolve_name(self, serializer: Serializer) -> Optional[str]:
        for name, s in self._name_registry.items():
            if serializer == s:
                return name
        return None
