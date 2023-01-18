import datetime
from abc import ABC, abstractmethod
from typing import Optional, Sequence, AsyncIterable

from ai.lzy.v1.whiteboard.whiteboard_pb2 import Whiteboard
from lzy.whiteboards.wrapper import WhiteboardWrapper


class WhiteboardIndexClient(ABC):  # pragma: no cover
    @abstractmethod
    async def get(self, id_: str) -> Optional[Whiteboard]:
        pass

    @abstractmethod
    async def query(
        self,
        name: Optional[str] = None,
        tags: Sequence[str] = (),
        not_before: Optional[datetime.datetime] = None,
        not_after: Optional[datetime.datetime] = None
    ) -> AsyncIterable[Whiteboard]:
        yield  # type: ignore

    @abstractmethod
    async def register(self, wb: Whiteboard) -> None:
        pass

    @abstractmethod
    async def update(self, wb: Whiteboard):
        pass


class WhiteboardManager(ABC):  # pragma: no cover
    @abstractmethod
    async def write_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def update_meta(self, wb: Whiteboard, uri: str, storage_name: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def get(self,
                  *,
                  id_: Optional[str],
                  storage_uri: Optional[str] = None,
                  storage_name: Optional[str] = None) -> Optional[WhiteboardWrapper]:
        pass

    @abstractmethod
    async def query(self,
                    *,
                    name: Optional[str] = None,
                    tags: Sequence[str] = (),
                    not_before: Optional[datetime.datetime] = None,
                    not_after: Optional[datetime.datetime] = None,
                    storage_uri: Optional[str] = None,
                    storage_name: Optional[str] = None) -> AsyncIterable[WhiteboardWrapper]:
        yield  # type: ignore
