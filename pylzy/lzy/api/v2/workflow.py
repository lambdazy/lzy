import dataclasses
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Type, TypeVar

from lzy.api.v2.snapshot import Snapshot
from lzy.api.v2.whiteboard_declaration import fetch_whiteboard_meta

T = TypeVar("T")  # pylint: disable=invalid-name

if TYPE_CHECKING:
    from lzy.api.v2.call import LzyCall
    from lzy.api.v2.lzy import Lzy
    from lzy.api.v2.runtime import WhiteboardField


class LzyWorkflow:
    instance: Optional["LzyWorkflow"] = None

    @classmethod
    def get_active(cls) -> "LzyWorkflow":
        assert cls.instance is not None, "There is no active LzyWorkflow"
        return cls.instance

    def __init__(
        self, name: str, owner: "Lzy", snapshot: Snapshot, eager: bool = False
    ):
        self.__snapshot = snapshot
        self.__name = name
        self.__eager = eager
        self.__owner = owner
        self.__call_queue: List["LzyCall"] = []

    @property
    def owner(self) -> "Lzy":
        return self.__owner

    @property
    def snapshot(self) -> Snapshot:
        return self.snapshot

    @property
    def name(self) -> str:
        return self.__name

    def register_call(self, call: "LzyCall") -> Any:
        self.__call_queue.append(call)
        if self.__eager:
            self.barrier()

    def barrier(self) -> None:
        # TODO[ottergottaott]: prepare tasks before?
        # seems it's better to prepare them inside of runtime
        # graph = prepare_tasks_and_channels(self._id, self._call_queue)
        self.__owner.runtime.exec(self.__call_queue, lambda x: print(x))
        self.__call_queue = []

    def create_whiteboard(self, typ: Type[T], tags: Sequence = ()) -> T:
        declaration_meta = fetch_whiteboard_meta(typ)
        if declaration_meta is None:
            raise ValueError(
                f"Whiteboard class should be annotated with both @whiteboard or @dataclass"
            )

        declared_fields = dataclasses.fields(typ)
        fields = []
        for field in declared_fields:
            if field.default != dataclasses.MISSING:
                entry = self.__snapshot.create_entry(field.type)
                self.__snapshot.put_data(entry.id, field.default)
                fields.append(
                    WhiteboardField(field.name, self.__snapshot.resolve_url(entry.id))
                )

        created_meta = self.__owner.runtime.create_whiteboard(
            declaration_meta.namespace,
            declaration_meta.name,
            fields,
            self.__snapshot.storage_name(),
            tags,
        )
        # TODO (tomato): return constructed wb
        return typ()

    def __enter__(self) -> "LzyWorkflow":
        if type(self).instance is not None:
            raise RuntimeError("Simultaneous workflows are not supported")
        type(self).instance = self
        self.__owner.runtime.start(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.barrier()
        finally:
            self.__owner.runtime.destroy()
            type(self).instance = None
