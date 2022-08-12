import logging
import sys
from typing import Dict, List, Type, Optional, cast

import cloudpickle  # type: ignore
from pure_protobuf.dataclasses_ import load, loads, Message  # type: ignore

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
        self._dumper_priorities: Dict[str, int] = {}

        self.register_serializer(CatboostPoolSerializer(), self._default_priority)
        self.register_serializer(FileSerializer(), self._default_priority)

        self.register_serializer(ProtoMessageSerializer(), self._default_priority)
        self.register_serializer(CloudpickleSerializer(), sys.maxsize - 1)

    def register_serializer(self, dumper: Serializer, priority: Optional[int] = None) -> None:
        if not dumper.available():
            self._log.warning(f"Dumper {dumper.name()} cannot be imported")
            return

        if dumper.name() in self._dumper_priorities:
            self._log.warning(f"Dumper {dumper.name()} has been already imported")
            return

        priority = self._default_priority if priority is None else priority
        self._dumper_priorities[dumper.name()] = priority
        self._name_registry[dumper.name()] = dumper
        # mypy issue: https://github.com/python/mypy/issues/3060
        if isinstance(dumper.supported_types(), Type):  # type: ignore
            self._type_registry[cast(Type, dumper.supported_types())] = dumper
        else:
            self._filters_registry.append(dumper)

    def unregister_serializer(self, dumper: Serializer):
        if dumper.name() in self._dumper_priorities:
            del self._dumper_priorities[dumper.name()]
            del self._name_registry[dumper.name()]
            # mypy issue: https://github.com/python/mypy/issues/3060
            if isinstance(dumper.supported_types(), Type):  # type: ignore
                del self._type_registry[cast(Type, dumper.supported_types())]
            else:
                self._filters_registry[:] = [x for x in self._filters_registry if x.name() != dumper.name()]

    def find_serializer_by_type(self, typ: Type) -> Serializer:
        filter_dumper: Optional[Serializer] = None
        filter_dumper_priority = sys.maxsize
        for dumper in self._filters_registry:
            if dumper.supported_types()(typ) and self._dumper_priorities[dumper.name()] < filter_dumper_priority:
                filter_dumper_priority = self._dumper_priorities[dumper.name()]
                filter_dumper = dumper

        obj_type_dumper: Optional[Serializer] = self._type_registry[typ] if typ in self._type_registry else None
        obj_type_dumper_priority = sys.maxsize if obj_type_dumper is None else self._dumper_priorities[
            obj_type_dumper.name()]

        if filter_dumper is not None and obj_type_dumper is not None:
            return filter_dumper if filter_dumper_priority < obj_type_dumper_priority else obj_type_dumper
        elif filter_dumper is not None:
            return filter_dumper
        elif obj_type_dumper is not None:
            return obj_type_dumper
        else:
            raise ValueError(f"Could not find serializer for type {typ}")

    def find_serializer_by_name(self, dumper_name: str) -> Optional[Serializer]:
        if dumper_name in self._name_registry:
            return self._name_registry[dumper_name]
        return None
