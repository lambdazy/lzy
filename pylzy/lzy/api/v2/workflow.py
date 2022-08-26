from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

from lzy.env.env_provider import EnvProvider

if TYPE_CHECKING:
    from lzy.api.v2.call import LzyCall
    from lzy.api.v2.lzy import Lzy
    from lzy.api.v2.snapshot.snapshot import Snapshot


class LzyWorkflow:
    instance: Optional["LzyWorkflow"] = None

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert cls.instance is not None, "There is no active LzyWorkflow"
        return cls.instance

    def __init__(self, name: str, owner: "Lzy", eager: bool = False):
        self._name = name
        self._eager = eager
        self._owner = owner
        self._env_provider: EnvProvider = self._owner.env_provider
        self._runtime = self._owner.runtime
        self._call_queue: List["LzyCall"] = []

        self._id = str(uuid4())

    @property
    def owner(self) -> "Lzy":
        return self._owner

    @property
    def name(self) -> str:
        return self._name

    def register_call(self, call: "LzyCall") -> Any:
        self._call_queue.append(call)
        if self._eager:
            self.barrier()

    def barrier(self) -> None:
        # TODO[ottergottaott]: prepare tasks before?
        # seems it's better to prepare them inside of runtime
        # graph = prepare_tasks_and_channels(self._id, self._call_queue)
        self._runtime.exec(
            self._call_queue, lambda x: print(x)
        )
        self._call_queue = []

    def __enter__(self) -> "LzyWorkflow":
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self
        self._runtime.start(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.barrier()
        finally:
            self._runtime.destroy()
            type(self).instance = None
