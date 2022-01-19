from abc import ABC, abstractmethod
import json
from dataclasses import dataclass
from typing import Generic, List, Optional, TypeVar

from lzy.model.env import Env
from lzy.model.slot import Slot
from lzy.model.signatures import FuncSignature


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

    def tags(self) -> List[Tag]:
        res = []
        for v in self.__dict__.values():
            if v:
                res.append(v.tag())
        return res


T = TypeVar('T')


# Zygote should've been just marked as
# @dataclass(frozen=True)
# but mypy is broken here a bit, so workaround with mixin is needed:
# https://stackoverflow.com/questions/69330256/how-to-get-an-abstract-dataclass-to-pass-mypy
# https://github.com/python/mypy/issues/5374#issuecomment-568335302
@dataclass
class ZygoteDataclassMixin(Generic[T]):
    signature: FuncSignature[T]
    arg_slots: List[Slot]
    return_slot: Slot
    env: Optional[Env]
    provisioning: Optional[Provisioning]


class Zygote(ZygoteDataclassMixin[T], ABC):
    @property
    def slots(self) -> List[Slot]:
        return self.arg_slots + [self.return_slot]

    @property
    def name(self) -> str:
        return self.signature.name

    @property
    def description(self) -> str:
        return self.name

    @property
    @abstractmethod
    def command(self) -> str:
        pass

    def to_json(self) -> str:
        env = self.env
        provisioning = self.provisioning
        return json.dumps({
            # tried to serialize env as json and it didn't work,
            # so build dict here instead for env
            "env": {env.type_id(): env.as_dct()} if env else {},
            "fuze": self.command,
            "provisioning": {"tags": [
                {"tag": tag}
                for tag in provisioning.tags()
            ]} if provisioning else {},
            "slots": [
                slot.to_dict() for slot in self.slots
            ],
            "description": self.description
        }, sort_keys=True, indent=3)
