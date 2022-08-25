from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Any

from lzy._proxy.result import Result

from lzy.api.v2.call import LzyCall
from lzy.api.v2.snapshot.snapshot import Snapshot


@dataclass
class ProgressStep:
    pass


class Runtime(ABC):

    @abstractmethod
    def start(self):
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
