from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

from lzy.api.v2.grpc.graph_executor_client import prepare_tasks_and_channels
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.env.env_provider import EnvProvider

if TYPE_CHECKING:
    from lzy.api.v2.api import LzyCall
    from lzy.api.v2.api.lzy import Lzy


class LzyWorkflow:
    instance: Optional["LzyWorkflow"] = None

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert cls.instance is not None, "There is no active LzyWorkflow"
        return cls.instance

    def __init__(self, name: str, lzy_mount: str, owner: "Lzy", eager: bool = False):
        self._name = name
        self._eager = eager
        self._owner = owner
        self._env_provider: EnvProvider = self._owner.env_provider
        self._lzy_mount = lzy_mount
        self._runtime = self._owner.runtime
        self._snapshot = self._owner.snapshot_provider.get(
            lzy_mount, self._owner._serializer
        )
        self._call_queue: List["LzyCall"] = []

        self._id = str(uuid4())

    @property
    def owner(self) -> "Lzy":
        return self._owner

    def snapshot(self) -> Snapshot:
        return self._snapshot

    def register_call(self, call: "LzyCall") -> Any:
        self._call_queue.append(call)
        if self._eager:
            self.barrier()

    def barrier(self) -> None:
        graph = prepare_tasks_and_channels(self._id, self._call_queue)
        self._runtime.exec(graph, self._snapshot, lambda: print("progress"))
        self._call_queue = []

    def __enter__(self) -> "LzyWorkflow":
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self
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
            type(self).instance = None
