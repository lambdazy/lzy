import asyncio
import dataclasses
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar, cast, )

from lzy.whiteboards.api import WhiteboardField, WhiteboardDefaultDescription

from lzy.api.v1.env import Env
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.snapshot import Snapshot
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, lzy_proxy
from lzy.api.v1.whiteboards import WritableWhiteboard, fetch_whiteboard_meta, WbRef
from lzy.proxy.result import Just
from lzy.py_env.api import PyEnv
from lzy.utils.event_loop import LzyEventLoop

T = TypeVar("T")  # pylint: disable=invalid-name

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
            namespace: Dict[str, Any],
            snapshot: Snapshot,
            env: Env,
            *,
            eager: bool = False,
            provisioning: Provisioning = Provisioning.default(),
            interactive: bool = True,
    ):
        self.__snapshot = snapshot
        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__call_queue: List["LzyCall"] = []
        self.__whiteboards_links: Dict[str, WbRef] = {}
        self.__started = False

        self.__auto_py_env: PyEnv = owner.env_provider.provide(namespace)
        self.__default_env: Env = env

        self.__provisioning = provisioning
        self.__interactive = interactive
        self.__whiteboards: List[str] = []

    @property
    def owner(self) -> "Lzy":
        return self.__owner

    @property
    def snapshot(self) -> Snapshot:
        return self.__snapshot

    @property
    def name(self) -> str:
        return self.__name

    @property
    def auto_py_env(self) -> PyEnv:
        return self.__auto_py_env

    @property
    def default_env(self) -> Env:
        return self.__default_env

    @property
    def provisioning(self) -> Provisioning:
        return self.__provisioning

    @property
    def is_interactive(self) -> bool:
        return self.__interactive

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def add_whiteboard_link(self, storage_uri: str, ref: WbRef):
        self.__whiteboards_links[storage_uri] = ref

    def barrier(self):
        LzyEventLoop.run_async(self._barrier())

    def create_whiteboard(self, typ: Type[T], *, tags: Sequence = ()) -> T:
        return LzyEventLoop.run_async(self.__create_whiteboard(typ, tags))

    def __enter__(self) -> "LzyWorkflow":
        try:
            LzyEventLoop.run_async(self.__start())
            return self
        except Exception as e:
            self.__destroy()
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not self.__started:
                raise RuntimeError("Workflow not started")
            if exc_type != LzyExecutionException:
                LzyEventLoop.run_async(self._barrier())
        finally:
            self.__destroy()

    def __destroy(self):
        try:
            LzyEventLoop.run_async(self.__stop())
        finally:
            type(self).instance = None
            self.__started = False

    async def __stop(self):
        await self.__owner.runtime.destroy()
        wbs_to_finalize = []
        while len(self.__whiteboards) > 0:
            wb_id = self.__whiteboards.pop()
            wbs_to_finalize.append(self.__owner.whiteboard_client.finalize(wb_id))
        await asyncio.gather(*wbs_to_finalize)

    async def __start(self):
        if self.__started:
            return RuntimeError("Workflow already started")
        self.__started = True
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self
        await self.__owner.runtime.start(self)

    async def _barrier(self) -> None:
        if len(self.__call_queue) == 0:
            return

        data_to_load = []
        for call in self.__call_queue:
            for arg, eid in zip(call.args, call.arg_entry_ids):
                if not is_lzy_proxy(arg):
                    data_to_load.append(self.snapshot.put_data(eid, arg))

            for name, kwarg in call.kwargs.items():
                eid = call.kwarg_entry_ids[name]
                if not is_lzy_proxy(kwarg):
                    data_to_load.append(self.snapshot.put_data(eid, kwarg))

        await asyncio.gather(*data_to_load)

        await self.__owner.runtime.exec(self.__call_queue, self.__whiteboards_links, lambda x: print(x))
        self.__call_queue = []

    async def __create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
        declaration_meta = fetch_whiteboard_meta(typ)
        if declaration_meta is None:
            raise ValueError(
                f"Whiteboard class should be annotated with both @whiteboard and @dataclass"
            )

        declared_fields = dataclasses.fields(typ)
        fields = []

        data_to_load = []

        defaults = {}

        for field in declared_fields:
            if field.default != dataclasses.MISSING:
                entry = self.snapshot.create_entry(field.type)
                data_to_load.append(self.snapshot.put_data(entry.id, field.default))
                fields.append(
                    WhiteboardField(field.name, WhiteboardDefaultDescription(entry.storage_url, entry.data_scheme))
                )
                defaults[field.name] = lzy_proxy(entry.id, field.type, self, Just(field.default))
            else:
                fields.append(
                    WhiteboardField(field.name)
                )

        await asyncio.gather(*data_to_load)

        created_meta = await self.__owner.whiteboard_client.create_whiteboard(
            declaration_meta.namespace,
            declaration_meta.name,
            fields,
            self.snapshot.storage_name(),
            tags,
        )

        self.__whiteboards.append(created_meta.id)
        wb = WritableWhiteboard(typ, created_meta, self, defaults)
        return cast(T, wb)
