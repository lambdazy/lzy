import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from lzy.api.v1.workflow import WbRef
from lzy.proxy.result import unwrap
from lzy.storage.api import StorageConfig

if TYPE_CHECKING:
    from lzy.api.v1 import LzyWorkflow

from lzy.api.v1.call import LzyCall
from lzy.api.v1.runtime import (
    ProgressStep,
    Runtime,
)


class LocalRuntime(Runtime):
    def __init__(self):
        self.__workflow: Optional["LzyWorkflow"] = None

    async def start(self, workflow: "LzyWorkflow"):
        self.__workflow = workflow
        self.__workflow.owner.storage_registry.register_storage(
            "default", StorageConfig.yc_object_storage("bucket", "", "")
        )

    async def exec(
        self,
        calls: List[LzyCall],
        _: Dict[str, WbRef],
        progress: Callable[[ProgressStep], None],
    ):
        assert self.__workflow is not None
        graph: Dict[str, List[LzyCall]] = defaultdict(list)
        eid_to_call: Dict[str, LzyCall] = {}
        used: Dict[str, bool] = defaultdict(lambda: False)

        for call in calls:
            for eid in call.arg_entry_ids:
                graph[eid].append(call)

            for eid in call.kwarg_entry_ids.values():
                graph[eid].append(call)

            for eid in call.entry_ids:
                eid_to_call[eid] = call

        ans: List[str] = []

        # noinspection PyShadowingNames
        def dfs(eid: str):
            for c in graph[eid]:
                for edge in c.entry_ids:
                    if not used[edge]:
                        used[edge] = True
                        dfs(edge)
            ans.append(eid)

        for eid in eid_to_call.keys():
            if not used[eid]:
                dfs(eid)

        ans.reverse()
        mat: Dict[str, bool] = defaultdict(lambda: False)

        for eid in ans:
            c = eid_to_call[eid]
            if mat[c.id]:
                continue
            await self.__exec_call(c)
            mat[c.id] = True

    async def __exec_call(self, call: LzyCall):
        assert self.__workflow is not None

        args: List[Any] = [
            unwrap(data)
            for data in await asyncio.gather(
                *[self.__workflow.snapshot.get_data(eid) for eid in call.arg_entry_ids]
            )
        ]
        kwargs: Dict[str, Any] = {
            name: unwrap(await self.__workflow.snapshot.get_data(eid))
            for (name, eid) in call.kwarg_entry_ids.items()
        }

        value = call.signature.func.callable(*args, **kwargs)
        if len(call.entry_ids) == 1:
            await self.__workflow.snapshot.put_data(call.entry_ids[0], value)
            return

        data_to_put = []

        for i, data in enumerate(value):
            data_to_put.append(
                self.__workflow.snapshot.put_data(call.entry_ids[i], data)
            )

        await asyncio.gather(*data_to_put)

    async def destroy(self) -> None:
        pass
