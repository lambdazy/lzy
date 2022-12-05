import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from serialzy.api import Schema

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard


@dataclass
class WhiteboardDefaultDescription:
    url: str
    data_scheme: Schema


@dataclass
class WhiteboardField:
    name: str
    default: Optional[WhiteboardDefaultDescription] = None


@dataclass
class WhiteboardInstanceMeta:
    id: str
    name: str
    tags: Sequence[str]


class WhiteboardClient(ABC):
    @abstractmethod
    async def get(self, wb_id: str) -> Whiteboard:
        pass

    @abstractmethod
    async def list(
            self,
            name: Optional[str] = None,
            tags: Sequence[str] = (),
            not_before: Optional[datetime.datetime] = None,
            not_after: Optional[datetime.datetime] = None
    ) -> Iterable[Whiteboard]:
        pass

    @abstractmethod
    async def create_whiteboard(
            self,
            namespace: str,
            name: str,
            fields: Sequence[WhiteboardField],
            storage_name: str,
            tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:
        pass

    @abstractmethod
    async def link(self, wb_id: str, field_name: str, url: str, data_scheme: Schema) -> None:
        pass

    @abstractmethod
    async def finalize(
            self,
            whiteboard_id: str
    ):
        pass
