from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

from lzy.env.env_provider import EnvProvider

if TYPE_CHECKING:
    from lzy.api.v2.call import LzyCall
    from lzy.api.v2.lzy import Lzy


class LzyWorkflow:
    instance: Optional["LzyWorkflow"] = None

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert cls.instance is not None, "There is no active LzyWorkflow"
        return cls.instance

    def __init__(self, name: str, owner: "Lzy", eager: bool = False):
        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__env_provider: EnvProvider = self.__owner.env_provider
        self.__runtime = self.__owner.runtime
        self.__call_queue: List["LzyCall"] = []

    @property
    def owner(self) -> "Lzy":
        return self.__owner

    @property
    def name(self) -> str:
        return self.__name

    @property
    def env_provider(self) -> EnvProvider:
        return self.__env_provider

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def barrier(self) -> None:
        # TODO[ottergottaott]: prepare tasks before?
        # seems it's better to prepare them inside of runtime
        # graph = prepare_tasks_and_channels(self._id, self._call_queue)
        self.__runtime.exec(self.__call_queue, lambda x: print(x))
        self.__call_queue = []

    def __enter__(self) -> "LzyWorkflow":
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self
        self.__runtime.start(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.barrier()
        finally:
            self.__runtime.destroy()
            type(self).instance = None
