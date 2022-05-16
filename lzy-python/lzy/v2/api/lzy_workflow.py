from typing import Any, List, Optional

from lzy.v2.api import LzyCall
from lzy.v2.api.env.env_provider import EnvProvider
from lzy.v2.api.lzy_workflow_splitter import LzyWorkflowSplitter
from lzy.v2.api.runtime.local_runtime import LocalRuntime
from lzy.v2.api.runtime.runtime import Runtime
from lzy.v2.api.snapshot.snapshot import Snapshot
from lzy.v2.serialization.serializer import Serializer


class LzyWorkflow:
    instances: List["LzyWorkflow"] = []

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert len(cls.instances) > 0, "There is no active LzyWorkflow"
        return cls.instances[-1]

    def __init__(self,
                 name: str,
                 serializer: Serializer,
                 env_provider: EnvProvider,
                 lzy_mount: str,
                 eager: bool = False,
                 runtime: Runtime = LocalRuntime(),
                 snapshot: Optional[Snapshot] = None,
                 ):
        self._name = name
        self._eager = eager
        self._serializer = serializer
        self._env_provider = env_provider
        self._lzy_mount = lzy_mount
        self._ops: List[LzyCall] = []
        self._runtime = runtime
        self._snapshot = snapshot
        self._splitter = LzyWorkflowSplitter()

    def snapshot(self) -> Optional[Snapshot]:
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
                if self._snapshot is not None:
                    self._snapshot.finalize()
            else:
                if self._snapshot is not None:
                    self._snapshot.error()
        finally:
            self._runtime.destroy()
            type(self).instances.pop()
