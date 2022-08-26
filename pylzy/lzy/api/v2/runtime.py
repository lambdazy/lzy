from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Any

from lzy._proxy.result import Result
from typing import TYPE_CHECKING

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
    ) -> None:
        pass

    @abstractmethod
    def resolve_data(self, entry_id: str) -> Result[Any]:
        pass

    @abstractmethod
    def destroy(self) -> None:
        pass
