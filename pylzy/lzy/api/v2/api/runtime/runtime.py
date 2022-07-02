from dataclasses import dataclass
from typing import Callable

from lzy.api.v2.api.graph import Graph
from lzy.api.v2.api.snapshot.snapshot import Snapshot

@dataclass
class ProgressStep:
    pass

class Runtime:
    def exec(
        self, graph: Graph, snapshot: Snapshot, progress: Callable[[ProgressStep,], None]
    ) -> None:
        pass

    def destroy(self) -> None:
        pass
