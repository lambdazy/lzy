from typing import Callable

from lzy.v2.api.graph import Graph
from lzy.v2.api.runtime.runtime import Runtime
from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.utils import is_lazy_proxy


class LocalRuntime(Runtime):
    def exec(self, graph: Graph, snapshot: Snapshot, progress: Callable[[], None]):
        for call in graph.calls():
            args = tuple(
                arg if not is_lazy_proxy(arg) else arg._origin
                for arg in call.args
            )
            kwargs = {
                name: arg if not is_lazy_proxy(arg) else arg._origin
                for name, arg in call.kwargs.items()
            }
            x = call.op.callable(*args, **kwargs)
            a = 2

    def destroy(self) -> None:
        pass
