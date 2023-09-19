from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    cast,
    Set,
    ClassVar,
)
from typing_extensions import Self

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard

from lzy.api.v1.snapshot import Snapshot, DefaultSnapshot
from lzy.api.v1.utils.hashing import md5_of_str
from lzy.api.v1.utils.validation import is_name_valid, NAME_VALID_SYMBOLS
from lzy.api.v1.whiteboards import WritableWhiteboard
from lzy.api.v1.remote.lzy_service_client import USER_ENV
from lzy.utils.event_loop import LzyEventLoop

from lzy.env.base import WithLogger
from lzy.env.environment import LzyEnvironment
from lzy.env.mixin import WithEnvironmentMixin

T = TypeVar("T")

if TYPE_CHECKING:
    from lzy.api.v1 import Lzy
    from lzy.core.call import LzyCall


@dataclass
class LzyWorkflow(WithEnvironmentMixin, WithLogger):
    instance: ClassVar[Optional[LzyWorkflow]] = None
    name: str
    owner: Lzy
    env: LzyEnvironment
    eager: bool = False
    interactive: bool = True

    @classmethod
    def get_active(cls) -> Optional[LzyWorkflow]:
        return cls.instance

    @classmethod
    def set_active(cls, instance: Optional[LzyWorkflow]) -> None:
        # To support LzyWorkflow inheritance and Lzy._workflow_class override
        # from the one hand and to support LzyWorkflow.get_active() return
        # active descendant to the other hand.
        for klass in cls.__mro__:
            if issubclass(klass, LzyWorkflow):
                klass.instance = instance

    def __post_init__(self) -> None:
        super().__post_init__()
        if not is_name_valid(self.name):
            raise ValueError(f"Invalid workflow name. Name can contain only {NAME_VALID_SYMBOLS}")

        # TODO: move auth credentials
        self.__user = os.getenv(USER_ENV)

        self.__whiteboards: List[WritableWhiteboard] = []
        self.__filled_entry_ids: Set[str] = set()
        self.__call_queue: List[LzyCall] = []
        self.__started = False
        self.__snapshot: Optional[Snapshot] = None
        self.__execution_id: Optional[str] = None

    def with_fields(self, **kwargs: Any) -> Self:
        # we need to forbid changing Provisioning, Env, etc
        # after workflow started because it may lead
        # to surprises and special effects,
        # not to mention that after starting it obtains
        # internal state which we can't clone.
        if (
            self.__whiteboards or
            self.__filled_entry_ids or
            self.__call_queue or
            self.__started or
            self.__snapshot or
            self.__execution_id
        ):
            raise RuntimeError(
                "It's forbidden to change LzyWorkflow fields after "
                "start working with it"
            )

        return super().with_fields(**kwargs)

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
    def filled_entry_ids(self) -> Set[str]:
        return self.__filled_entry_ids

    @property
    def call_queue(self) -> List[LzyCall]:
        return self.__call_queue

    def register_call(self, call: LzyCall) -> Any:
        self.__call_queue.append(call)
        if self.eager:
            self.barrier()

    def barrier(self):
        LzyEventLoop.run_async(self._barrier())

    def create_whiteboard(self, typ: Type[T], *, tags: Sequence = ()) -> T:
        wb = WritableWhiteboard(typ, tags, self)
        self.__whiteboards.append(wb)
        return cast(T, wb)

    def __enter__(self) -> Self:
        try:
            parts = [self.owner.storage_uri]
            # user may not be set in tests
            if self.__user is not None:
                parts.append(self.__user)
            parts.extend(['lzy_runs', self.name, 'inputs/'])
            storage_uri = '/'.join(parts)

            self.__execution_id = LzyEventLoop.run_async(self.__start())
            self.__snapshot = DefaultSnapshot(
                self.owner.serializer_registry,
                storage_uri,
                self.owner.storage_client,
                self.owner.storage_name
            )
            return self
        except Exception:  # pylint: disable=broad-exception-caught
            try:
                self.__abort()
            finally:
                raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not self.__started:
                self.log.warning("Exiting workflow that has not been started")
                return
            if exc_val is None:
                try:
                    self.barrier()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    exc_val = e
        finally:
            if exc_val is None:
                self.__destroy()
            else:
                try:
                    self.__abort()
                finally:
                    raise exc_val

    def __abort(self):
        self.log.info(f"Abort workflow '{self.name}'")
        try:
            LzyEventLoop.run_async(self.owner.runtime.abort())
        finally:
            self.set_active(None)
            self.__started = False

    def __destroy(self):
        try:
            LzyEventLoop.run_async(self.__stop())
        finally:
            self.set_active(None)
            self.__started = False

    async def __stop(self):
        self.log.info(f"Finishing workflow '{self.name}'")
        await self.owner.runtime.finish()
        wbs_to_finalize = []
        while self.__whiteboards:
            whiteboard = self.__whiteboards.pop()
            wbs_to_finalize.append(
                self.owner.whiteboard_manager.update_meta(
                    Whiteboard(
                        id=whiteboard.id,
                        status=Whiteboard.Status.FINALIZED
                    ),
                    uri=whiteboard.storage_uri
                )
            )
        await asyncio.gather(*wbs_to_finalize)

    async def __start(self) -> str:
        if self.__started:
            raise RuntimeError("Workflow already started")
        if self.get_active() is not None:
            raise RuntimeError("Simultaneous workflows are not supported")

        self.log.info(f"Starting workflow '{self.name}'")
        workflow_id = await self.owner.runtime.start(self)
        self.__started = True
        self.set_active(self)
        return workflow_id

    async def _barrier(self) -> None:
        if self.__call_queue:
            self.log.info(
                f"Building graph from calls "
                f"{' -> '.join(call.signature.func.callable.__name__ for call in self.__call_queue)}"
            )

            for call in self.__call_queue:
                if call.cache:
                    self.__gen_results_uri_in_cache(call)
                else:
                    self.__gen_results_uri_skip_cache(call)

            await self.owner.runtime.exec(
                self.__call_queue,
                lambda x: self.log.info(f"Graph status: {x.name}")
            )
            self.__call_queue = []

        await self.__fill_whiteboards_proxy_fields()

    async def __fill_whiteboards_proxy_fields(self):
        wb_copy_tasks = []
        for wb in self.__whiteboards:
            self.log.debug(f"Fill whiteboard {wb.name} proxy fields by materialized op data")
            for key, from_eid in wb.proxy_fields.items():
                wb_copy_tasks.append(self.snapshot.copy_data(from_eid, f"{wb.storage_uri}/{key}"))

        await asyncio.gather(*wb_copy_tasks)

    def __gen_results_uri_in_cache(self, call: LzyCall):
        args_hashes = [self.snapshot.get(eid).data_hash for eid in call.arg_entry_ids]
        kwargs_hashes = []
        for name in sorted(call.kwargs.keys()):
            kwargs_hashes.append(f"{name}:{self.snapshot.get(call.kwarg_entry_ids[name]).data_hash}")

        inputs_hashes_concat = '_'.join([*args_hashes, *kwargs_hashes])

        parts = [
            self.owner.storage_uri,
            # user may not be set in tests
            self.__user or '',
            'lzy_runs',
            self.name,
            'ops',
            call.callable_name,
            call.version,
            inputs_hashes_concat
        ]
        uri_prefix = '/'.join(filter(bool, parts))

        for i, eid in enumerate(call.entry_ids):
            if eid not in self.__filled_entry_ids:
                entry = self.snapshot.get(eid)
                entry.storage_uri = uri_prefix + f"/return_{str(i)}"
                entry.data_hash = md5_of_str(entry.storage_uri)
                self.__filled_entry_ids.add(eid)

        eid = call.exception_id
        if eid not in self.__filled_entry_ids:
            entry = self.snapshot.get(eid)
            uri_suffix = f"{call.callable_name}/{call.id}/exception"
            entry.storage_uri = f"{self.owner.storage_uri}/lzy_runs/{self.name}/ops/" + uri_suffix
            entry.data_hash = md5_of_str(entry.storage_uri)
            self.__filled_entry_ids.add(eid)

    def __gen_results_uri_skip_cache(self, call: 'LzyCall'):
        for i, eid in enumerate(call.entry_ids):
            if eid not in self.__filled_entry_ids:
                entry = self.snapshot.get(eid)
                uri_suffix = f"{call.callable_name}/{call.id}/return_{str(i)}"
                entry.storage_uri = f"{self.owner.storage_uri}/lzy_runs/{self.name}/ops/" + uri_suffix
                entry.data_hash = md5_of_str(entry.storage_uri)
                self.__filled_entry_ids.add(eid)

        eid = call.exception_id
        if eid not in self.__filled_entry_ids:
            entry = self.snapshot.get(eid)
            uri_suffix = f"{call.callable_name}/{call.id}/exception"
            entry.storage_uri = f"{self.owner.storage_uri}/lzy_runs/{self.name}/ops/" + uri_suffix
            entry.data_hash = md5_of_str(entry.storage_uri)
            self.__filled_entry_ids.add(eid)
