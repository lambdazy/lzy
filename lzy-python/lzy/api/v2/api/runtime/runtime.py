from typing import Callable

from lzy.api.v2.api.graph import Graph
from lzy.api.v2.api.snapshot.snapshot import Snapshot


class Runtime:
    def exec(self, graph: Graph, snapshot: Snapshot, progress: Callable[[], None]) -> None:
        pass

    def destroy(self) -> None:
        pass
