import inspect
import logging
import pathlib
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
from datetime import timezone
from inspect import Signature
from typing import Dict, List, Optional, Any, Type, TypeVar
from urllib import parse

from lzy.api.v1.servant.model.slot import Slot

T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass
class SnapshotDescription:
    snapshot_id: str


class WhiteboardFieldStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardFieldDescription:
    field_name: str
    status: WhiteboardFieldStatus
    dependent_field_names: Optional[List[str]]  # protobuf makes no distinction between empty list and null list
    storage_uri: str


class WhiteboardStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardDescription:
    id: str  # pylint: disable=invalid-name
    fields: List[WhiteboardFieldDescription]
    snapshot: SnapshotDescription
    status: WhiteboardStatus


class WhiteboardList:
    def __init__(self, wb_list: List[Any]):
        self.wb_list = wb_list
        self._log = logging.getLogger(str(self.__class__))

    @staticmethod
    def _methods_with_view_decorator_names(obj) -> List:
        res = []
        for name in dir(obj):
            attribute = getattr(obj, name)
            if hasattr(attribute, 'LZY_WB_VIEW_DECORATOR'):
                res.append(name)
        return res

    def _views_from_single_whiteboard(self, wb, view_type: Type[T]):
        method_names = self._methods_with_view_decorator_names(wb)
        all_methods = []
        for method_name in method_names:
            all_methods.append(getattr(wb, method_name))
        methods_to_call = []
        for method in all_methods:
            return_type = inspect.signature(method).return_annotation
            if return_type == Signature.empty or return_type != view_type:
                continue
            methods_to_call.append(method)
        return [method() for method in methods_to_call]

    def views(self, view_type: Type[T]) -> List[T]:
        res = []
        for elem in self.wb_list:
            if elem.__status__ == WhiteboardStatus.COMPLETED:
                res.extend(self._views_from_single_whiteboard(elem, view_type))
            else:
                self._log.warning(
                    f"Whiteboard with id={elem.__id__} is not completed and is not used for building view")
        return res

    def __iter__(self):
        return iter(self.wb_list)

    def __getitem__(self, key):
        return self.wb_list.__getitem__(key)

    def __len__(self):
        return len(self.wb_list)


class SnapshotApi(ABC):
    @abstractmethod
    def create(self, workflow_name: str) -> SnapshotDescription:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str):
        pass

    @abstractmethod
    def last(self, workflow_name: str) -> Optional[SnapshotDescription]:
        pass


class WhiteboardApi(ABC):
    @abstractmethod
    def create(self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]) -> WhiteboardDescription:
        pass

    @abstractmethod
    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    @abstractmethod
    def get(self, wb_id: str) -> WhiteboardDescription:
        pass

    @abstractmethod
    def list(self, namespace: str, tags: List[str], from_date: datetime = None, to_date: datetime = None) -> List[WhiteboardDescription]:
        pass

    @abstractmethod
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        pass


class EntryIdGenerator(ABC):
    @abstractmethod
    def generate(self, slot: str) -> str:
        pass


class UUIDEntryIdGenerator(EntryIdGenerator):
    def __init__(self, snapshot_id: str) -> None:
        self.__snapshot_id = snapshot_id

    def generate(self, slot_name: str) -> str:
        return "/".join([self.__snapshot_id, slot_name, str(uuid.uuid1())])


class InMemWhiteboardApi(WhiteboardApi):
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        return None

    def __init__(self) -> None:
        self.__whiteboards: Dict[str, WhiteboardDescription] = {}
        self.__namespaces: Dict[str, str] = {}
        self.__tags: Dict[str, List[str]] = {}
        self.__creation_date: Dict[str, datetime] = {}

    def create(self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]) -> WhiteboardDescription:
        wb_id = str(uuid.uuid1())
        self.__whiteboards[wb_id] = WhiteboardDescription(
            wb_id,
            [WhiteboardFieldDescription(name, WhiteboardFieldStatus.CREATED, [], None) for name in fields], # type: ignore
            SnapshotDescription(snapshot_id=snapshot_id),
            WhiteboardStatus.CREATED,
        )
        self.__namespaces[wb_id] = namespace
        self.__tags[wb_id] = tags
        self.__creation_date[wb_id] = datetime.now(timezone.utc)
        return self.__whiteboards[wb_id]

    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    def get(self, wb_id: str) -> WhiteboardDescription:
        return self.__whiteboards[wb_id]

    def list(self, namespace: str, tags: List[str], from_date: datetime = None, to_date: datetime = None) \
            -> List[WhiteboardDescription]:
        if not from_date:
            from_date = datetime(1, 1, 1, tzinfo=timezone.utc)
        if not to_date:
            to_date = datetime(9999, 12, 31, tzinfo=timezone.utc)
        from_utc = datetime.fromtimestamp(from_date.timestamp(), tz=timezone.utc)
        to_utc = datetime.fromtimestamp(to_date.timestamp(), tz=timezone.utc)
        namespace_ids = [k for k, v in self.__namespaces.items() if v == namespace]
        tags_ids = [k for k, v in self.__tags.items() if all(item in v for item in tags)]
        wb_ids = set.intersection(set(namespace_ids), set(tags_ids))
        wb_ids_filtered_by_date = [id for id in wb_ids if from_utc <= self.__creation_date[id] < to_utc]
        return [self.__whiteboards[id_] for id_ in wb_ids_filtered_by_date]


class InMemSnapshotApi(SnapshotApi):
    def create(self, workflow_name: str) -> SnapshotDescription:
        return SnapshotDescription(str(uuid.uuid1()))

    def finalize(self, snapshot_id: str):
        pass

    def last(self, workflow_name: str) -> SnapshotDescription:
        return SnapshotDescription("")


def get_bucket_from_url(url: str) -> str:
    uri = parse.urlparse(url)
    path = pathlib.PurePath(uri.path)
    if path.is_absolute():
        return path.parts[1]
    else:
        return path.parts[0]
