from abc import ABC, abstractmethod
from typing import TypeVar, IO

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyDumper(ABC):
    @abstractmethod
    def dump(self, obj: T, dest: IO) -> None:
        pass

    @abstractmethod
    def load(self, source: IO) -> T:
        pass