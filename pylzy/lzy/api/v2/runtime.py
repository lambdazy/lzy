from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Optional, Sequence

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.call import LzyCall


@dataclass
class ProgressStep:
    pass


@dataclass
class WhiteboardField:
    name: str
    url: Optional[str]


@dataclass
class WhiteboardInstanceMeta:
    id: str


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
    def destroy(self) -> None:
        pass

    @abstractmethod
    def link(self, wb_id: str, field_name: str, url: str) -> None:
        pass

    @abstractmethod
    def create_whiteboard(
        self,
        namespace: str,
        name: str,
        fields: Sequence[WhiteboardField],
        storage_name: str,
        tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:
        pass
