from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Dict

from lzy.api.v1.workflow import WbRef

if TYPE_CHECKING:
    from lzy.api.v1 import LzyWorkflow

from lzy.api.v1.call import LzyCall


@dataclass
class ProgressStep:
    pass


class Runtime(ABC):
    @abstractmethod
    async def start(self, workflow: "LzyWorkflow"):
        pass

    @abstractmethod
    async def exec(
        self,
        calls: List[LzyCall],
        links: Dict[str, WbRef],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        pass

    @abstractmethod
    async def destroy(self) -> None:
        pass
