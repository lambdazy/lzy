import logging
import os
import tempfile
import uuid
from typing import BinaryIO, Type, TypeVar, Union, Callable, Any

import cloudpickle
from lzy.serialization.api import Serializer
from pure_protobuf.dataclasses_ import Message  # type: ignore

from lzy.serialization.types import File

T = TypeVar("T")  # pylint: disable=invalid-name


# noinspection PyPackageRequirements,PyMethodMayBeStatic
class CatboostPoolSerializer(Serializer):
    def name(self) -> str:
        return "CATBOOST_POOL_SERIALIZER"

    def __init__(self):
        self._log = logging.getLogger(str(self.__class__))

    def available(self) -> bool:
        # noinspection PyBroadException
        try:
            import catboost  # type: ignore

            return True
        except:
            logging.warning("Cannot import catboost")
            return False

    def serialize(self, obj: T, dest: BinaryIO) -> None:
        with tempfile.NamedTemporaryFile() as handle:
            if not obj.is_quantized():  # type: ignore
                obj.quantize()  # type: ignore
            obj.save(handle.name)  # type: ignore
            while True:
                data = handle.read(8096)
                if not data:
                    break
                dest.write(data)

    def deserialize(self, source: BinaryIO) -> T:
        with tempfile.NamedTemporaryFile() as handle:
            while True:
                data = source.read(8096)
                if not data:
                    break
                handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
            import catboost

            return catboost.Pool("quantized://" + handle.name)  # type: ignore

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        import catboost
        return catboost.Pool  # type: ignore

    def stable(self) -> bool:
        return True


# noinspection PyMethodMayBeStatic
class FileSerializer(Serializer):
    def name(self) -> str:
        return "FILE_SERIALIZER"

    def serialize(self, obj: File, dest: BinaryIO) -> None:
        with obj.path.open("rb") as f:
            data = f.read(4096)
            while len(data) > 0:
                dest.write(data)
                data = f.read(4096)

    def deserialize(self, source: BinaryIO) -> File:
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


# noinspection PyMethodMayBeStatic
class CloudpickleSerializer(Serializer):
    def name(self) -> str:
        return "CLOUDPICKLE_SERIALIZER"

    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO) -> Any:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return False

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: True


# noinspection PyMethodMayBeStatic
class ProtoMessageSerializer(Serializer):
    def name(self) -> str:
        return "PROTO_MESSAGE_SERIALIZER"

    def serialize(self, obj: Message, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO) -> Message:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda t: issubclass(t, Message)
