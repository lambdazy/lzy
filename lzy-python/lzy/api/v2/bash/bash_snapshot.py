import dataclasses
import os
import uuid
from enum import Enum
from typing import Type, TypeVar, Any, Dict, Set, Callable, Optional

from lzy.api.v2.servant.whiteboard_api import WhiteboardApi
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.serialization.serializer import Serializer
from lzy.api.v2.servant.snapshot_api import SnapshotApi
from lzy.api.v2.servant.channel_manager import ChannelManager
from lzy.api.v2.utils import is_lazy_proxy

T = TypeVar("T")  # pylint: disable=invalid-name


class SnapshotStatus(Enum):
    CREATED = "CREATED"
    FINALIZED = "FINALIZED"
    ERRORED = "ERRORED"


class SnapshotException(Exception):
    pass


ALREADY_WRAPPED = '_already_wrapped_whiteboard'
ALREADY_WRAPPED_READY = '_already_wrapped_ready_whiteboard'

WB_ID_GETTER_NAME = '__id_getter__'
LZY_FIELDS_ASSIGNED = '__lzy_fields_assigned__'


def create_instance(typ: Type[T]) -> T:
    if not dataclasses.is_dataclass(typ):
        raise ValueError(f"Expected a dataclass; got {typ} instead")
    field_types = {field.name: field.type for field in dataclasses.fields(typ)}
    field_dict: Dict[str, Any] = {}
    for field_name, field_type in field_types:
        field_dict[field_name] = None
    return typ(**field_dict)


class BashSnapshot(Snapshot):
    def __init__(self, snapshot_id: str, lzy_mount: str, snapshot_api_client: SnapshotApi,
                 whiteboard_api_client: WhiteboardApi, channel_manager: ChannelManager, serializer: Serializer):
        self._lzy_mount = lzy_mount
        self._id = snapshot_id
        self._silent = False
        self._whiteboards = []
        self._snapshot_api_client = snapshot_api_client
        self._whiteboard_api_client = whiteboard_api_client
        self._channel_manager = channel_manager
        self._serializer = serializer
        self._status: SnapshotStatus = SnapshotStatus.CREATED

    def id(self) -> str:
        return self._id

    def _wrap_whiteboard(
            self,
            instance: Any,
            whiteboard_id_getter: Callable[[], Optional[str]]
    ):
        if not dataclasses.is_dataclass(instance):
            raise ValueError(f"Expected a dataclass; got {type(instance)} instead")
        fields = dataclasses.fields(instance)
        fields_dict: Dict[str, dataclasses.Field] = {
            field.name: field
            for field in fields
        }

        fields_assigned: Set[str] = set()

        def __setattr__(self: Any, key: str, value: Any):
            if not hasattr(self, ALREADY_WRAPPED):
                super(type(self), self).__setattr__(key, value)
                return

            if key not in fields_dict:
                raise AttributeError(f"No such attribute: {key}")

            if key in fields_assigned:
                raise ValueError('Whiteboard field can be assigned only once')

            if is_lazy_proxy(value):
                # noinspection PyProtectedMember
                return_entry_id = value._call.entry_id  # pylint: disable=protected-access
                whiteboard_id = whiteboard_id_getter()
                if return_entry_id is None or whiteboard_id is None:
                    raise RuntimeError("Cannot get entry_id from op")

                self._whiteboard_api_client.link(whiteboard_id, key, return_entry_id)
            else:
                entry_id = key + '_' + str(uuid.uuid4())
                whiteboard_id = whiteboard_id_getter()
                if whiteboard_id is None:
                    raise RuntimeError("Cannot get whiteboard id")
                path = self._channel_manager.out_slot(entry_id)
                with path.open("wb") as handle:
                    self._serializer.serialize_to_file(value, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                self._whiteboard_api_client.link(whiteboard_id, key, entry_id)

            fields_assigned.add(key)
            # interesting fact: super() doesn't work in outside-defined functions
            # as it works in methods of classes
            # and we actually need to pass class and instance here
            super(type(instance), self).__setattr__(key, value)

        setattr(instance, WB_ID_GETTER_NAME, whiteboard_id_getter)
        setattr(instance, LZY_FIELDS_ASSIGNED, fields_assigned)
        setattr(instance, ALREADY_WRAPPED, True)
        type(instance).__setattr__ = __setattr__  # type: ignore

    def shape(self, wb_type: Type[T]) -> T:
        if self._status == SnapshotStatus.ERRORED or self._status == SnapshotStatus.FINALIZED:
            raise SnapshotException(f"Invoking method shape in snapshot with status {self._status} is forbidden")
        instance: T = create_instance(wb_type)
        whiteboard_id: str = str(uuid.uuid4())
        return self._wrap_whiteboard(instance, lambda: whiteboard_id)

    def get(self, entry_id: str) -> Any:
        # TODO: add implementation
        pass

    def silent(self) -> None:
        self._silent = True

    def finalize(self):
        whiteboards = self._whiteboards
        for whiteboard in whiteboards:
            fields = dataclasses.fields(whiteboard)
            for field in fields:
                if field.name not in whiteboard.__lzy_fields_assigned__:
                    value = getattr(whiteboard, field.name)
                    setattr(whiteboard, field.name, value)

        if self._status == SnapshotStatus.ERRORED:
            raise SnapshotException(f"Finalizing snapshot in error condition is forbidden")
        self._status = SnapshotStatus.FINALIZED
        self._snapshot_api_client.finalize(self._id)

    def error(self):
        if self._status == SnapshotStatus.FINALIZED:
            raise SnapshotException(f"Setting snapshot status to error in finalized snapshot is forbidden")
        self._status = SnapshotStatus.ERRORED
        self._snapshot_api_client.error(self._id)
