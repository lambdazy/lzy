from dataclasses import dataclass

from .base_internal import BaseInternal


@dataclass
class Base:
    a: int
    b: str

    @staticmethod
    def echo() -> str:
        return BaseInternal.echo()
