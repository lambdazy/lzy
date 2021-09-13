from abc import abstractmethod
from typing import Any, Callable


class LzyRunner:
    @abstractmethod
    def run(self, func: Callable, *args) -> Any:
        pass


class LocalLzyRunner(LzyRunner):
    def run(self, func: Callable, *args) -> Any:
        return func(*args)
