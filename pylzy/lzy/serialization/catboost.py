import logging
import os
import tempfile
from typing import BinaryIO, Callable, Dict, Type, TypeVar, Union

from lzy.serialization.api import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


# noinspection PyPackageRequirements,PyMethodMayBeStatic
class CatboostPoolSerializer(Serializer):
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

    def deserialize(self, source: BinaryIO, typ: Type) -> T:
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

    def format(self) -> str:
        import catboost

        return str(catboost.Pool.__module__) + "." + str(catboost.Pool.__name__)

    def meta(self) -> Dict[str, str]:
        import catboost

        return {"catboost_version": catboost.__version__}
