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
    TypeVar, Set, cast, Mapping,
)

from lzy.api.v2.event_loop import LzyEventLoop
from lzy.proxy.result import Just

from lzy.api.v2.env import Env
from lzy.api.v2.provisioning import Provisioning
from lzy.api.v2.snapshot import Snapshot
from lzy.api.v2.utils.proxy_adapter import is_lzy_proxy, get_proxy_entry_id, lzy_proxy
from lzy.api.v2.whiteboard_declaration import fetch_whiteboard_meta, WhiteboardField, WhiteboardInstanceMeta
from lzy.py_env.api import PyEnv

T = TypeVar("T")  # pylint: disable=invalid-name

if TYPE_CHECKING:
    from lzy.api.v2 import Lzy
    from lzy.api.v2.call import LzyCall


@dataclasses.dataclass
class WbRef:
    whiteboard_id: str
    field_name: str


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

    def create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
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
            LzyEventLoop.run_async(self._barrier())
        finally:
            self.__destroy()

    def __destroy(self):
        try:
            LzyEventLoop.run_async(self.__owner.runtime.destroy())
        finally:
            type(self).instance = None
            self.__started = False

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
                    WhiteboardField(field.name, self.snapshot.resolve_url(entry.id))
                )
                defaults[field.name] = lzy_proxy(entry.id, field.type, self, Just(field.default))

        await asyncio.gather(*data_to_load)

        created_meta = await self.__owner.runtime.create_whiteboard(
            declaration_meta.namespace,
            declaration_meta.name,
            fields,
            self.snapshot.storage_name(),
            tags,
        )

        wb = _WritableWhiteboard(typ, created_meta, self, defaults)

        return cast(T, wb)


class _WritableWhiteboard:
    __internal_fields = {
        "_Whiteboard__fields_dict", "_Whiteboard__fields_assigned", "_Whiteboard__whiteboard_meta",
        "_Whiteboard__workflow", "_Whiteboard__fields",
    }

    def __init__(
            self,
            instance: Any,
            whiteboard_meta: "WhiteboardInstanceMeta",
            wf: LzyWorkflow,
            fields: Mapping[str, Any]
    ):
        self.__fields_dict: Dict[str, dataclasses.Field] = {
            field.name: field for field in dataclasses.fields(instance)
        }

        self.__fields_assigned: Set[str] = set()
        self.__whiteboard_meta = whiteboard_meta
        self.__workflow = wf
        self.__fields: Dict[str, Any] = {}
        self.__fields.update(fields)

    def __setattr__(self, key: str, value: Any):
        if key in _WritableWhiteboard.__internal_fields:  # To complete constructor
            super(_WritableWhiteboard, self).__setattr__(key, value)
            return

        if key not in self.__fields_dict:
            raise AttributeError(f"No such attribute: {key}")

        if key in self.__fields_assigned:
            raise AttributeError("Whiteboard field can be assigned only once")

        whiteboard_id = self.__whiteboard_meta.id

        if is_lzy_proxy(value):
            entry_id = get_proxy_entry_id(value)
            entry = self.__workflow.snapshot.get(entry_id)
            self.__workflow.add_whiteboard_link(entry.storage_url, WbRef(whiteboard_id, key))
        else:
            entry = self.__workflow.snapshot.create_entry(type(value))
            LzyEventLoop.run_async(self.__workflow.snapshot.put_data(entry_id=entry.id, data=value))
            value = lzy_proxy(entry.id, type(value), self.__workflow, Just(value))
            LzyEventLoop.run_async(self.__workflow.owner.runtime.link(whiteboard_id, key, entry.storage_url))

        self.__fields_assigned.add(key)

        self.__fields[key] = value

    def __getattr__(self, item: str) -> Any:
        if item not in self.__fields:
            raise AttributeError(f"Whiteboard has no field {item}")
        return self.__fields[item]

    @property
    def whiteboard_id(self) -> str:
        return self.__whiteboard_meta.id

