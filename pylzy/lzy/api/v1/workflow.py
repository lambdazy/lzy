import asyncio
import dataclasses
import datetime
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar, cast, Set, )

from serialzy.api import Schema

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from lzy.api.v1.env import Env
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.snapshot import Snapshot, DefaultSnapshot
from lzy.api.v1.utils.proxy_adapter import is_lzy_proxy, lzy_proxy
from lzy.api.v1.whiteboards import WritableWhiteboard, fetch_whiteboard_meta
from lzy.logs.config import get_logger
from lzy.proxy.result import Just
from lzy.py_env.api import PyEnv
from lzy.utils.event_loop import LzyEventLoop
from ai.lzy.v1.whiteboard.whiteboard_pb2 import WhiteboardField, Whiteboard, Storage
# noinspection PyPackageRequirements
from google.protobuf.timestamp_pb2 import Timestamp

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
        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__call_queue: List["LzyCall"] = []
        self.__started = False

        self.__env = env
        self.__provisioning = provisioning
        self.__auto_py_env = auto_py_env
        self.__interactive = interactive
        self.__whiteboards: List[Whiteboard] = []
        self.__filled_entry_ids: Set[str] = set()

        self.__snapshot: Optional[Snapshot] = None
        self.__execution_id: Optional[str] = None

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

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def barrier(self):
        LzyEventLoop.run_async(self._barrier())

    def create_whiteboard(self, typ: Type[T], *, tags: Sequence = ()) -> T:
        return LzyEventLoop.run_async(self.__create_whiteboard(typ, tags))

    def __enter__(self) -> "LzyWorkflow":
        try:
            self.__execution_id = LzyEventLoop.run_async(self.__start())
            self.__snapshot = DefaultSnapshot(self.owner.serializer_registry, self.owner.storage_client,
                                              self.owner.storage_name)
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
        _LOG.info(f"Finishing workflow '{self.name}'")
        await self.__owner.runtime.destroy()
        wbs_to_finalize = []
        while len(self.__whiteboards) > 0:
            whiteboard = self.__whiteboards.pop()
            wbs_to_finalize.append(
                self.__owner.whiteboard_manager.update_meta(
                    Whiteboard(id=whiteboard.id, status=Whiteboard.Status.FINALIZED), uri=whiteboard.storage.uri))
        await asyncio.gather(*wbs_to_finalize)

    async def __start(self) -> str:
        if self.__started:
            raise RuntimeError("Workflow already started")
        self.__started = True
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self

        _LOG.info(f"Starting workflow '{self.name}'")
        return await self.__owner.runtime.start(self)

    async def _barrier(self) -> None:
        if len(self.__call_queue) == 0:
            return

        _LOG.info(f"Building graph from calls "
                  f"{' -> '.join(call.signature.func.callable.__name__ for call in self.__call_queue)}")

        data_to_load = []
        for call in self.__call_queue:
            for arg, eid in zip(call.args, call.arg_entry_ids):
                if not is_lzy_proxy(arg):
                    data_to_load.append(self.snapshot.put_data(eid, arg))

            for name, kwarg in call.kwargs.items():
                eid = call.kwarg_entry_ids[name]
                if not is_lzy_proxy(kwarg):
                    data_to_load.append(self.snapshot.put_data(eid, kwarg))

            self.__filled_entry_ids.update(call.entry_ids)

        await asyncio.gather(*data_to_load)
        await self.__owner.runtime.exec(self.__call_queue, lambda x: _LOG.info(f"Graph status: {x.name}"))
        self.__call_queue = []

    async def __create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
        declaration_meta = fetch_whiteboard_meta(typ)
        if declaration_meta is None:
            raise ValueError(
                f"Whiteboard class should be annotated with both @whiteboard and @dataclass"
            )

        config = self.owner.storage_registry.default_config()
        if config is None:
            raise ValueError("Default storage is required to create whiteboard")

        whiteboard_id = str(uuid.uuid4())
        storage_prefix = f"whiteboards/{declaration_meta.name}-{whiteboard_id}"
        whiteboard_uri = f"{config.uri}/{storage_prefix}"

        declared_fields = dataclasses.fields(typ)
        fields = []
        data_to_load = []
        defaults = {}

        for field in declared_fields:
            serializer = self.owner.serializer_registry.find_serializer_by_type(field.type)
            if not serializer.available():
                raise ValueError(
                    f'Serializer for type {field.type} is not available, please install {serializer.requirements()}')
            if not serializer.stable():
                raise ValueError(
                    f'Variables of type {field.type} cannot be assigned on whiteboard'
                    f' because we cannot serialize them in a portable format. '
                    f'See https://github.com/lambdazy/serialzy for details.')

            if field.default != dataclasses.MISSING:
                entry = self.snapshot.create_entry(declaration_meta.name + "." + field.name, field.type,
                                                   f"{whiteboard_uri}/{field.name}.default")
                data_to_load.append(self.snapshot.put_data(entry.id, field.default))
                defaults[field.name] = lzy_proxy(entry.id, (field.type,), self, Just(field.default))

            fields.append(WhiteboardField(name=field.name, scheme=self.__build_scheme(serializer.schema(field.type))))

        await asyncio.gather(*data_to_load)

        now = datetime.datetime.now()
        timestamp = Timestamp()
        timestamp.FromDatetime(now)
        whiteboard = Whiteboard(
            id=whiteboard_id,
            name=declaration_meta.name,
            tags=tags,
            fields=fields,
            storage=Storage(name=self.owner.storage_name, uri=whiteboard_uri),
            namespace="default",
            status=Whiteboard.Status.CREATED,
            createdAt=timestamp
        )
        await self.owner.whiteboard_manager.write_meta(whiteboard, whiteboard_uri)
        self.__whiteboards.append(whiteboard)
        wb = WritableWhiteboard(typ, whiteboard, self, defaults)
        return cast(T, wb)

    @staticmethod
    def __build_scheme(data_scheme: Schema) -> DataScheme:
        return DataScheme(
            dataFormat=data_scheme.data_format,
            schemeFormat=data_scheme.schema_format,
            schemeContent=data_scheme.schema_content
            if data_scheme.schema_content else "",
            metadata=data_scheme.meta
        )
