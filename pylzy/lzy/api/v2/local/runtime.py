from typing import TYPE_CHECKING, Callable, List, Optional

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.call import LzyCall
from lzy.api.v2.proxy_adapter import is_lzy_proxy
from lzy.api.v2.runtime import ProgressStep, Runtime


class LocalRuntime(Runtime):
    def __init__(self):
        self.__wflow: Optional["LzyWorkflow"] = None

    def start(self, workflow):
        pass

    def exec(
        self,
        graph: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ):
        assert self.__wflow is not None
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
            if len(call.entry_ids) == 0:
                self.__wflow.owner.snapshot.put_data(call.entry_ids[0], value)
                continue
            for i, data in enumerate(value):
                self.__wflow.owner.snapshot.put_data(call.entry_ids[i], data)

    def destroy(self) -> None:
        pass
