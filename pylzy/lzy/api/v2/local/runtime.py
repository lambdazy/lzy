import uuid
from typing import TYPE_CHECKING, Callable, List, Optional, Sequence

from lzy.storage.api import StorageConfig

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.call import LzyCall
from lzy.api.v2.runtime import (
    ProgressStep,
    Runtime,
    WhiteboardField,
    WhiteboardInstanceMeta,
)
from lzy.api.v2.utils.proxy_adapter import is_lzy_proxy


class LocalRuntime(Runtime):
    def __init__(self):
        self.__workflow: Optional["LzyWorkflow"] = None

    def start(self, workflow: "LzyWorkflow"):
        self.__workflow = workflow
        self.__workflow.owner.storage_registry.register_storage(
            "default", StorageConfig.yc_object_storage("bucket", "", "")
        )

    def exec(
        self,
        graph: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ):
        assert self.__workflow is not None
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
                self.__workflow.snapshot.put_data(call.entry_ids[0], value)
                continue
            for i, data in enumerate(value):
                self.__workflow.snapshot.put_data(call.entry_ids[i], data)

    def destroy(self) -> None:
        pass

    def link(self, wb_id: str, field_name: str, url: str) -> None:
        pass

    def create_whiteboard(
        self,
        namespace: str,
        name: str,
        fields: Sequence[WhiteboardField],
        storage_name: str,
        tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:
        return WhiteboardInstanceMeta(str(uuid.uuid4()))
