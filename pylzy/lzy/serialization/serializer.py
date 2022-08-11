from abc import abstractmethod
from typing import Any, BinaryIO, Dict, List, Type, TypeVar, cast

import cloudpickle  # type: ignore
from pure_protobuf.dataclasses_ import load, loads  # type: ignore

from lzy.serialization.api import Dumper
from lzy.serialization.dumper import CatboostPoolDumper, LzyFileDumper

T = TypeVar("T")  # pylint: disable=invalid-name


# TODO[ottergottaott]: drop it
def check_message_field(obj: Any) -> bool:
    if obj is None:
        return False
    return hasattr(obj, "LZY_MESSAGE")


class FileSerializer:
    def __init__(self):
        self._registry: Dict[Type, Dumper] = {}
        dumpers: List[Dumper] = [CatboostPoolDumper(), LzyFileDumper()]
        for dumper in dumpers:
            if dumper.fit():
                self._registry[dumper.typ()] = dumper

    def serialize(self, obj: Any, file: BinaryIO) -> None:
        typ = (
            type(obj)
            if not hasattr(obj, "__lzy_origin__")
            else type(obj.__lzy_origin__)
        )
        if typ in self._registry:
            dumper = self._registry[typ]
            dumper.dump(obj, file)
        elif check_message_field(typ) or check_message_field(obj):
            obj.dump(file)  # type: ignore
        else:
            cloudpickle.dump(obj, file)

    def deserialize(self, data: BinaryIO, obj_type: Type[T] = None) -> T:
        if obj_type in self._registry:
            dumper = self._registry[obj_type]
            return cast(T, dumper.load(data))
        elif check_message_field(obj_type):
            return load(obj_type, data)  # type: ignore
        return cloudpickle.load(data)  # type: ignore

    def add_dumper(self, dumper: Dumper):
        if dumper.fit():
            self._registry[dumper.typ()] = dumper
