from typing import Callable, List

from lzy.api.v2.api.lzy_call import LzyCall
from lzy.api.v2.api.runtime.runtime import ProgressStep, Runtime
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.api.v2.proxy_adapter import is_lzy_proxy


class LocalRuntime(Runtime):
    def exec(
        self,
        graph: List[LzyCall],
        snapshot: Snapshot,
        progress: Callable[[ProgressStep], None],
    ):
        for call in graph:
            args = tuple(
                # TODO[ottergottaott]: hide this __lzy_origin__ attr access
                arg if not is_lzy_proxy(arg) else arg.__lzy_origin__
                for arg in call.args
            )
            kwargs = {
                name: arg if not is_lzy_proxy(arg) else arg.__lzy_origin__
                for name, arg in call.kwargs.items()
            }

            value = call.signature.func.callable(*args, **kwargs)
            snapshot.put(call.entry_id, value)

    def destroy(self) -> None:
        pass
