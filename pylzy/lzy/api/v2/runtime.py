from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.call import LzyCall


@dataclass
class ProgressStep:
    pass


class Runtime(ABC):
    @abstractmethod
    def start(self, workflow: "LzyWorkflow"):
        pass

    @abstractmethod
    def exec(
        self,
        graph: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ):
        pass

    @abstractmethod
    def destroy(self):
        pass
