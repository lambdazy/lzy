from dataclasses import dataclass
from typing import Optional


class Gpu:
    def __init__(self, is_any: bool = False):
        super().__init__()
        self._any = is_any

    @property
    def is_any(self):
        return self._any

    @staticmethod
    def any():
        return Gpu(True)


@dataclass
class Provisioning:
    gpu: Optional[Gpu] = None
