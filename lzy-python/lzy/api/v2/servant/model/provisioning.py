from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List


class Tag(ABC):
    @abstractmethod
    def tag(self) -> str:
        pass


class Gpu(Tag):
    def __init__(self, is_any: bool = False):
        super().__init__()
        self._any = is_any

    def tag(self) -> str:
        if self._any:
            return "GPU:ANY"
        return "GPU"

    @staticmethod
    def any():
        return Gpu(True)


@dataclass
class Provisioning:
    gpu: Optional[Gpu] = None

    def tags(self) -> List[str]:
        res = []
        if self.gpu:
            res.append(self.gpu.tag())
        return res
