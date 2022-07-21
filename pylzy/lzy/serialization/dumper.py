import os
import tempfile
import uuid
from typing import IO, Type, TypeVar

from lzy.serialization.api import Dumper
from lzy.serialization.types import File

T = TypeVar("T")  # pylint: disable=invalid-name


# noinspection PyPackageRequirements
class CatboostPoolDumper(Dumper):
    def fit(self) -> bool:
        # noinspection PyBroadException
        try:
            import catboost  # type: ignore

            return True
        except:
            return False

    def dump(self, obj: T, dest: IO) -> None:
        with tempfile.NamedTemporaryFile() as handle:
            if not obj.is_quantized():  # type: ignore
                obj.quantize()  # type: ignore
            obj.save(handle.name)  # type: ignore
            while True:
                data = handle.read(8096)
                if not data:
                    break
                dest.write(data)

    def load(self, source: IO) -> T:
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

    def typ(self) -> Type[T]:
        import catboost

        return catboost.Pool  # type: ignore


class LzyFileDumper(Dumper[File]):

    def dump(self, obj: File, dest: IO) -> None:
        with obj.path.open("rb") as f:
            data = f.read(4096)
            while len(data) > 0:
                dest.write(data)
                data = f.read(4096)

    def load(self, source: IO) -> File:
        new_path = os.path.join("/tmp", str(uuid.uuid1()))
        with open(new_path, "wb") as f:
            data = source.read(4096)
            while len(data) > 0:
                f.write(data)
                data = source.read(4096)
        return File(new_path)

    def typ(self) -> Type[File]:
        return File

    def fit(self) -> bool:
        return True
