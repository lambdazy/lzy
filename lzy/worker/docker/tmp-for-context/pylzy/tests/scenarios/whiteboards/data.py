from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional

from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32


@message
@dataclass
class Test1:
    a: int32 = field(1, default=0)


class TestEnum(IntEnum):
    BAR = 1
    FOO = 2
    BAZ = 3


@dataclass
class Rule:
    a: int
    b: str


@message
@dataclass
class MessageClass:
    string_field: str = field(1, default="")
    int_field: int32 = field(2, default=int32(0))
    list_field: List[int32] = field(3, default_factory=list)
    optional_field: Optional[int32] = field(4, default=None)
    inner_field: Test1 = field(5, default_factory=Test1)
    enum_field: TestEnum = field(6, default=TestEnum.BAR)
