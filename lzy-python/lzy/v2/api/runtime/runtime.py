from typing import Callable, Optional

from lzy.v2.api.graph import Graph
from lzy.v2.api.snapshot.snapshot import Snapshot


class Runtime:
    def exec(self, graph: Graph, snapshot: Optional[Snapshot], progress: Callable[[], None]) -> None:
        pass

    def destroy(self) -> None:
        pass
