from typing import Callable, List, Any, Dict

from lzy._proxy.result import Result, Nothing, Just

from lzy.api.v2.call import LzyCall
from lzy.api.v2.proxy_adapter import is_lzy_proxy
from lzy.api.v2.runtime import ProgressStep, Runtime


class LocalRuntime(Runtime):
    def __init__(self):
        self.__data: Dict[str, Any] = {}

    def start(self, workflow):
        pass

    def exec(
        self,
        graph: List[LzyCall],
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
            if len(call.entry_ids) == 0:
                self.__data[call.entry_ids[0]] = value
                continue
            for i, data in enumerate(value):
                self.__data[call.entry_ids[i]] = data

    def resolve_data(self, entry_id: str) -> Result[Any]:
        res = self.__data.get(entry_id, Nothing())
        if not isinstance(res, Nothing):
            return Just(res)
        return Nothing()

    def destroy(self) -> None:
        pass
