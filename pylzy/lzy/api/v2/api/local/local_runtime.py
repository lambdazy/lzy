from typing import Callable, List

from lzy.api.v2.api import LzyCall
from lzy.api.v2.api.runtime.runtime import Runtime
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.api.v2.proxy_adapter import is_lzy_proxy


class LocalRuntime(Runtime):
    def exec(
        self, graph: List[LzyCall], snapshot: Snapshot, progress: Callable[[], None]
    ):
        for call in graph.calls():
            args = tuple(
                arg if not is_lzy_proxy(arg) else arg.__lzy_origin__
                for arg in call.args
            )
            kwargs = {
                name: arg if not is_lzy_proxy(arg) else arg.__lzy_origin__
                for name, arg in call.kwargs.items()
            }
            snapshot.put(call.entry_id, call.op.callable(*args, **kwargs))

    def destroy(self) -> None:
        pass
