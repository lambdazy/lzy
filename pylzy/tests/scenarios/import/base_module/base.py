from dataclasses import dataclass

from lzy.api.v1 import op
from .base_internal import BaseInternal
from .internal.internal import print_internal


@op
def internal_op() -> None:
    print_internal()


@dataclass
class Base:
    a: int
    b: str

    @staticmethod
    def echo() -> str:
        return BaseInternal.echo()
