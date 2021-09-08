import typing as ty
from enum import Enum
from typing import List


class DataItem(dict):
    def id(self) -> str:
        return ""

    def page(self) -> str:
        return ""

    def index(self) -> int:
        return 0


Item = ty.TypeVar('Item', bound=DataItem)


class DataPage(ty.List[Item]):
    url: str # storage url
    type: type # stored items types
    generated: str # url to execution/channel
    depends_on: List[str]

    def is_valid(self) -> bool:
        return True

    def weak(self, ref):
        pass

    def soft(self, ref):
        pass

    def strong(self, wb, name):
        pass


class RelationType(Enum):
    STRONG = 'strong'
    WEAK = 'weak'


class Relation:
    depends: DataPage
    type: RelationType


class DataEntity(DataPage[Item]):
    def relations(self) -> List[Relation]:
        return []


