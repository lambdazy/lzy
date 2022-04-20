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
    def loadable(self) -> bool:
        pass


# noinspection PyPackageRequirements
class CatboostPoolDumper(Dumper):
    def loadable(self) -> bool:
        # noinspection PyBroadException
        try:
            import catboost
            return True
        except:
            return False

    def dump(self, obj: T, dest: IO) -> None:
        obj.save(dest.name)

    def load(self, source: IO) -> T:
        import catboost
        return catboost.Pool(source.name)

    def typ(self) -> Type[T]:
        import catboost
        return catboost.Pool
