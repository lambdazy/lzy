from typing import Any, List, TYPE_CHECKING

from lzy.v2.api import LzyCall
from lzy.v2.api.lzy_workflow_splitter import LzyWorkflowSplitter
from lzy.v2.api.snapshot.snapshot import Snapshot

if TYPE_CHECKING:
    from lzy.v2.api.lzy import Lzy


class LzyWorkflow:
    instances: List["LzyWorkflow"] = []

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert len(cls.instances) > 0, "There is no active LzyWorkflow"
        return cls.instances[-1]

    def __init__(self,
                 name: str,
                 lzy_mount: str,
                 owner: 'Lzy',
                 eager: bool = False
                 ):
        self._name = name
        self._eager = eager
        self._owner = owner
        self._env_provider = self._owner.env_provider
        self._lzy_mount = lzy_mount
        self._ops: List[LzyCall] = []
        self._runtime = self._owner.runtime
        self._snapshot = self._owner.snapshot_provider.get(lzy_mount, self._owner._serializer)
        self._splitter = LzyWorkflowSplitter()

    def owner(self) -> 'Lzy':
        return self._owner

    def snapshot(self) -> Snapshot:
        return self._snapshot

    def call(self, call: LzyCall) -> Any:
        self._splitter.call(call)
        if self._eager:
            self.barrier()

    def barrier(self) -> None:
        graph = self._splitter.barrier()
        self._runtime.exec(graph, self._snapshot, lambda: print("progress"))

    def __enter__(self) -> "LzyWorkflow":
        type(self).instances.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not exc_val:
                self.barrier()
                self._snapshot.finalize()
            else:
                self._snapshot.error()
        finally:
            self._runtime.destroy()
            type(self).instances.pop()
