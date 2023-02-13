from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Callable, List, Optional

from lzy.storage.api import Storage

if TYPE_CHECKING:
    from lzy.api.v1 import LzyWorkflow

from lzy.api.v1.call import LzyCall


class ProgressStep(Enum):
    WAITING = "WAITING"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Runtime(ABC):
    @abstractmethod
    async def start(self, workflow: "LzyWorkflow") -> str:
        pass

    @abstractmethod
    async def exec(
        self,
        calls: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        pass

    @abstractmethod
    async def abort(self) -> None:
        pass

    @abstractmethod
    async def finish(self) -> None:
        pass

    @abstractmethod
    async def storage(self) -> Optional[Storage]:
        pass
