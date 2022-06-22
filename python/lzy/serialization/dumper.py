import os
import tempfile
from abc import ABC, abstractmethod
from typing import TypeVar, IO, Type

T = TypeVar("T")  # pylint: disable=invalid-name


class Dumper(ABC):
    @abstractmethod
    def dump(self, obj: T, dest: IO) -> None:
        pass

    @abstractmethod
    def load(self, source: IO) -> T:
        pass

    @abstractmethod
    def typ(self) -> Type[T]:
        pass

    @abstractmethod
    def fit(self) -> bool:
        pass


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
