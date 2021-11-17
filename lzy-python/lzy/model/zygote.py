import abc
import json
from dataclasses import dataclass
from typing import List

from lzy.model.env import Env
from lzy.model.slot import Slot


class Tag:
    @abc.abstractmethod
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
    gpu: Gpu = None

    def tags(self) -> List[Tag]:
        res = []
        for v in self.__dict__.values():
            if v:
                res.append(v.tag())
        return res


class Zygote(abc.ABC):
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def command(self) -> str:
        pass

    @abc.abstractmethod
    def slots(self) -> List[Slot]:
        pass

    @abc.abstractmethod
    def env(self) -> Env:
        pass

    @abc.abstractmethod
    def provisioning(self) -> Provisioning:
        pass

    def to_json(self) -> str:
        return json.dumps({
            # tried to serialize env as json and it didn't work,
            # so build dict here instead for env
            "env": {self.env().type_id(): self.env().as_dct()},
            "fuze": self.command(),
            "provisioning": {"tags": [{"tag": tag} for tag in self.provisioning().tags()]},
            "slots": [
                slot.to_dict() for slot in self.slots()
            ]
        }, sort_keys=True, indent=3)
