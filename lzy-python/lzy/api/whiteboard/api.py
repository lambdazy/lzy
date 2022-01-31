import pathlib
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Type, TypeVar
from dataclasses import dataclass
from enum import Enum
import uuid
import logging
import inspect
from inspect import Signature
from urllib import parse

from lzy.model.slot import Slot

T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass
class SnapshotDescription:
    snapshot_id: str


class WhiteboardFieldStatus(Enum):
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"


@dataclass
class WhiteboardFieldDescription:
    field_name: str
    storage_uri: Optional[str]
    dependent_field_names: Optional[List[str]]


class WhiteboardStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    NOT_COMPLETED = "NOT_COMPLETED"
    ERRORED = "ERRORED"


@dataclass
class WhiteboardInfo:
    id: str  # pylint: disable=invalid-name
    status: Optional[WhiteboardStatus]


@dataclass
class WhiteboardDescription:
    id: str  # pylint: disable=invalid-name
    fields: List[WhiteboardFieldDescription]
    snapshot: Optional[SnapshotDescription]
    status: Optional[WhiteboardStatus]


class WhiteboardList:
    def __init__(self, wb_list):
        self.wb_list = wb_list

    def _methods_with_view_decorator_names(self, obj) -> List:
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
            res.extend(self._views_from_single_whiteboard(elem, view_type))
        return res

    def __iter__(self):
        return iter(self.wb_list)

    def __getitem__(self, key):
        return self.wb_list.__getitem__(key)

    def __len__(self):
        return len(self.wb_list)


class SnapshotApi(ABC):
    @abstractmethod
    def create(self) -> SnapshotDescription:
        pass

    @abstractmethod
    def finalize(self, snapshot_id: str):
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
    def get_by_namespace_and_tags(self, namespace: str, tags: List[str]) -> List[WhiteboardDescription]:
        pass

    @abstractmethod
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        pass

    @abstractmethod
    def get_all(self) -> List[WhiteboardInfo]:
        pass


class EntryIdGenerator(ABC):
    @abstractmethod
    def generate(self, slot: Slot) -> str:
        pass


class UUIDEntryIdGenerator(EntryIdGenerator):
    def __init__(self, snapshot_id: str) -> None:
        self.__snapshot_id = snapshot_id

    def generate(self, slot: Slot) -> str:
        return "/".join([self.__snapshot_id, slot.name, str(uuid.uuid1())])


class InMemWhiteboardApi(WhiteboardApi):
    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        return None

    def __init__(self) -> None:
        self.__whiteboards: Dict[str, WhiteboardDescription] = {}
        self.__namespaces: Dict[str, str] = {}
        self.__tags: Dict[str, List[str]] = {}

    def create(self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]) -> WhiteboardDescription:
        wb_id = str(uuid.uuid1())
        self.__whiteboards[wb_id] = WhiteboardDescription(
            wb_id,
            [WhiteboardFieldDescription(name, None, []) for name in fields],
            SnapshotDescription(snapshot_id=snapshot_id),
            WhiteboardStatus.CREATED,
        )
        self.__namespaces[wb_id] = namespace
        self.__tags[wb_id] = tags
        return self.__whiteboards[wb_id]

    def link(self, wb_id: str, field_name: str, entry_id: str):
        pass

    def get(self, wb_id: str) -> WhiteboardDescription:
        return self.__whiteboards[wb_id]

    def get_all(self) -> List[WhiteboardInfo]:
        return [
            WhiteboardInfo(wb.id, wb.status) for key, wb in self.__whiteboards.items()
        ]

    def get_by_namespace_and_tags(self, namespace: str, tags: List[str]) -> List[WhiteboardDescription]:
        namespace_ids = [k for k, v in self.__namespaces.items() if v == namespace]
        tags_ids = [k for k, v in self.__tags.items() if all(item in v for item in tags)]
        wb_ids = set.intersection(set(namespace_ids), set(tags_ids))
        return [self.__whiteboards[id] for id in wb_ids]


class InMemSnapshotApi(SnapshotApi):
    def create(self) -> SnapshotDescription:
        return SnapshotDescription(str(uuid.uuid1()))

    def finalize(self, snapshot_id: str):
        pass


def get_bucket_from_url(url: str) -> str:
    uri = parse.urlparse(url)
    path = pathlib.PurePath(uri.path)
    if path.is_absolute():
        return path.parts[1]
    else:
        return path.parts[0]