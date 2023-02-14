import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar, cast, Set, )

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard
from lzy.api.v1.entry_index import EntryIndex
from lzy.api.v1.env import Env
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.snapshot import Snapshot, DefaultSnapshot, SerializedDataHasher
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy
from lzy.api.v1.utils.validation import is_name_valid, NAME_VALID_SYMBOLS
from lzy.api.v1.whiteboards import WritableWhiteboard
from lzy.logs.config import get_logger
from lzy.py_env.api import PyEnv
from lzy.utils.event_loop import LzyEventLoop

# noinspection PyPackageRequirements

T = TypeVar("T")  # pylint: disable=invalid-name

_LOG = get_logger(__name__)
if TYPE_CHECKING:
    from lzy.api.v1 import Lzy
    from lzy.api.v1.call import LzyCall


class LzyWorkflow:
    instance: Optional["LzyWorkflow"] = None

    @classmethod
    def get_active(cls) -> Optional["LzyWorkflow"]:
        return cls.instance

    def __init__(
        self,
        name: str,
        owner: "Lzy",
        env: Env,
        provisioning: Provisioning,
        auto_py_env: PyEnv,
        *,
        eager: bool = False,
        interactive: bool = True
    ):
        if not is_name_valid(name):
            raise ValueError(f"Invalid workflow name. Name can contain only {NAME_VALID_SYMBOLS}")

        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__call_queue: List["LzyCall"] = []
        self.__started = False

        self.__env = env
        self.__provisioning = provisioning
        self.__auto_py_env = auto_py_env
        self.__interactive = interactive
        self.__whiteboards: List[WritableWhiteboard] = []
        self.__filled_entry_ids: Set[str] = set()

        self.__snapshot: Optional[Snapshot] = None
        self.__execution_id: Optional[str] = None
        self.__entry_index = EntryIndex()
        self.__hasher = SerializedDataHasher("sha256")

    @property
    def owner(self) -> "Lzy":
        return self.__owner

    @property
    def snapshot(self) -> Snapshot:
        if self.__snapshot is None:
            raise ValueError("Workflow is not yet started")
        return self.__snapshot

    @property
    def execution_id(self) -> str:
        if self.__execution_id is None:
            raise ValueError("Workflow is not yet started")
        return self.__execution_id

    @property
    def entry_index(self) -> EntryIndex:
        return self.__entry_index

    @property
    def name(self) -> str:
        return self.__name

    @property
    def env(self) -> Env:
        return self.__env

    @property
    def auto_py_env(self) -> PyEnv:
        return self.__auto_py_env

    @property
    def provisioning(self) -> Provisioning:
        return self.__provisioning

    @property
    def is_interactive(self) -> bool:
        return self.__interactive

    @property
    def filled_entry_ids(self) -> Set[str]:
        return self.__filled_entry_ids

    @property
    def call_queue(self) -> List["LzyCall"]:
        return self.__call_queue

    @property
    def eager(self) -> bool:
        return self.__eager

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def barrier(self):
        LzyEventLoop.run_async(self._barrier())

    def create_whiteboard(self, typ: Type[T], *, tags: Sequence = ()) -> T:
        wb = WritableWhiteboard(typ, tags, self)
        self.__whiteboards.append(wb)
        return cast(T, wb)

    def __enter__(self) -> "LzyWorkflow":
        try:
            self.__execution_id = LzyEventLoop.run_async(self.__start())
            self.__snapshot = DefaultSnapshot(self.__name, self.owner.serializer_registry, self.__owner.storage_uri,
                                              self.owner.storage_client, self.owner.storage_name, self.__hasher)
            return self
        except Exception as e:
            try:
                self.__abort()
            finally:
                raise e

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not self.__started:
                raise RuntimeError("Workflow not started")
            if exc_type is None:
                self.barrier()
        finally:
            if exc_type is None:
                self.__destroy()
            else:
                self.__abort()

    def __abort(self):
        _LOG.info(f"Abort workflow '{self.name}'")
        try:
            LzyEventLoop.run_async(self.__owner.runtime.abort())
        finally:
            type(self).instance = None
            self.__started = False

    def __destroy(self):
        try:
            LzyEventLoop.run_async(self.__stop())
        finally:
            type(self).instance = None
            self.__started = False

    async def __stop(self):
        _LOG.info(f"Finishing workflow '{self.name}'")
        await self.__owner.runtime.destroy()
        wbs_to_finalize = []
        while len(self.__whiteboards) > 0:
            whiteboard = self.__whiteboards.pop()
            wbs_to_finalize.append(
                self.__owner.whiteboard_manager.update_meta(
                    Whiteboard(id=whiteboard.id, status=Whiteboard.Status.FINALIZED), uri=whiteboard.storage_uri))
        await asyncio.gather(*wbs_to_finalize)

    async def __start(self) -> str:
        if self.__started:
            raise RuntimeError("Workflow already started")
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")

        _LOG.info(f"Starting workflow '{self.name}'")
        workflow_id = await self.__owner.runtime.start(self)
        self.__started = True
        type(self).instance = self
        return workflow_id

    async def _barrier(self) -> None:
        if len(self.__call_queue) == 0:
            return

        _LOG.info(f"Building graph from calls "
                  f"{' -> '.join(call.signature.func.callable.__name__ for call in self.__call_queue)}")

        data_to_load = []
        for call in self.__call_queue:
            for arg, eid in zip(call.args, call.arg_entry_ids):
                if not is_lzy_proxy(arg) and eid not in self.__filled_entry_ids:
                    data_to_load.append(self.snapshot.put_data(eid, arg))
                    self.__filled_entry_ids.add(eid)

            for name, kwarg in call.kwargs.items():
                eid = call.kwarg_entry_ids[name]
                if not is_lzy_proxy(kwarg) and eid not in self.__filled_entry_ids:
                    data_to_load.append(self.snapshot.put_data(eid, kwarg))
                    self.__filled_entry_ids.add(eid)

        await asyncio.gather(*data_to_load)

        for call in self.__call_queue:
            args_inputs_hash: str = '_'.join(map(lambda eid: self.snapshot.get(eid).hash, call.arg_entry_ids))
            kwargs_inputs_hash: str = '_'.join(map(
                lambda name: self.snapshot.get(call.kwarg_entry_ids[name]).hash,
                sorted(call.kwargs.keys())
            ))
            inputs_hash: str = '_'.join([args_inputs_hash, kwargs_inputs_hash])

            op_name: str = call.signature.func.callable.__name__
            op_version: str = '1.0'

            for i, eid in enumerate(call.entry_ids):
                if eid not in self.__filled_entry_ids:
                    self.snapshot.update_entry(eid, f"/${op_name}_${op_version}_${inputs_hash}/return_${str(i)}")
                    entry = self.snapshot.get(eid)
                    entry.set_hash(self.__hasher.hash_of_str(entry.storage_uri))
                    self.__filled_entry_ids.add(eid)

        await self.__owner.runtime.exec(self.__call_queue, lambda x: _LOG.info(f"Graph status: {x.name}"))

        await self.snapshot.processed_copying(self.owner.storage_client.copy)

        self.__call_queue = []
