import os
import uuid
from typing import Any, BinaryIO, Callable, Type, Union

from lzy.serialization.api import Serializer
from lzy.serialization.types import File


# noinspection PyMethodMayBeStatic
class FileSerializer(Serializer):
    def serialize(self, obj: File, dest: BinaryIO) -> None:
        with obj.path.open("rb") as f:
            data = f.read(4096)
            while len(data) > 0:
                dest.write(data)
                data = f.read(4096)

    def deserialize(self, source: BinaryIO, typ: Type) -> Any:
        new_path = os.path.join("/tmp", str(uuid.uuid1()))
        with open(new_path, "wb") as f:
            data = source.read(4096)
            while len(data) > 0:
                f.write(data)
                data = source.read(4096)
        return File(new_path)

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return File

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True
