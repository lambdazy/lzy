import asyncio
import dataclasses
from threading import Thread
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from lzy.api.v2.env import DockerPullPolicy, Env
from lzy.api.v2.snapshot import DefaultSnapshot, Snapshot
from lzy.api.v2.utils.env import generate_env
from lzy.api.v2.utils.proxy_adapter import is_lzy_proxy
from lzy.api.v2.whiteboard_declaration import fetch_whiteboard_meta
from lzy.py_env.api import PyEnv
from lzy.api.v2.provisioning import Provisioning

T = TypeVar("T")  # pylint: disable=invalid-name

if TYPE_CHECKING:
    from lzy.api.v2 import Lzy
    from lzy.api.v2.call import LzyCall
    from lzy.api.v2.runtime import WhiteboardField


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
        *,
        eager: bool = False,
        python_version: Optional[str] = None,
        libraries: Optional[Dict[str, str]] = None,
        conda_yaml_path: Optional[str] = None,
        docker_image: Optional[str] = None,
        docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
        local_modules_path: Optional[Sequence[str]] = None,
        provisioning: Optional[Provisioning] = None,
        zone: str = "ru-central1-a",
        interactive: bool = True
    ):
        self.__snapshot = snapshot
        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__call_queue: List["LzyCall"] = []
        self.__loop = asyncio.new_event_loop()
        self.__loop_thread = Thread(
            name="workflow-thread",
            target=self.__run_loop_thread,
            args=(self.__loop,),
            daemon=True,
        )
        self.__loop_thread.start()
        self.__started = False

        self.__auto_py_env: PyEnv = owner.env_provider.provide(namespace)
        self.__default_env: Env = generate_env(
            self.__auto_py_env,
            python_version,
            libraries,
            conda_yaml_path,
            docker_image,
            docker_pull_policy,
            local_modules_path,
        )

        self.__provisioning = provisioning
        self.__zone = zone
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
    def provisioning(self) -> Optional[Provisioning]:
        return self.__provisioning

    @property
    def zone(self) -> str:
        return self.__zone

    @property
    def is_interactive(self) -> bool:
        return self.__interactive

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def barrier(self):
        self._run_async(self._barrier())

    def create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
        return self._run_async(self.__create_whiteboard(typ, tags))

    def __enter__(self) -> "LzyWorkflow":
        try:
            self._run_async(self.__start())
            return self
        except Exception as e:
            self.__destroy()
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not self.__started:
                raise RuntimeError("Workflow not started")
            self._run_async(self._barrier())
        finally:
            self.__destroy()

    def _run_async(self, fun: Awaitable[T]) -> T:
        return asyncio.run_coroutine_threadsafe(fun, self.__loop).result()

    def __destroy(self):
        try:
            self._run_async(self.__owner.runtime.destroy())
        finally:
            self.__loop.call_soon_threadsafe(self.__loop.stop)
            self.__loop_thread.join()
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

    @staticmethod
    def __run_loop_thread(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

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

        await self.__owner.runtime.exec(self.__call_queue, lambda x: print(x))
        self.__call_queue = []

    async def __create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
        declaration_meta = fetch_whiteboard_meta(typ)
        if declaration_meta is None:
            raise ValueError(
                f"Whiteboard class should be annotated with both @whiteboard or @dataclass"
            )

        declared_fields = dataclasses.fields(typ)
        fields = []

        data_to_load = []

        for field in declared_fields:
            if field.default != dataclasses.MISSING:
                entry = self.snapshot.create_entry(field.type)
                data_to_load.append(self.snapshot.put_data(entry.id, field.default))
                fields.append(
                    WhiteboardField(field.name, self.snapshot.resolve_url(entry.id))
                )

        await asyncio.gather(*data_to_load)

        created_meta = self.__owner.runtime.create_whiteboard(
            declaration_meta.namespace,
            declaration_meta.name,
            fields,
            self.snapshot.storage_name(),
            tags,
        )
        # TODO (tomato): return constructed wb
        return typ()
