import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Optional, TypeVar

from lzy.api.v2.servant.model.env import Env
from lzy.api.v2.servant.model.provisioning import Provisioning
from lzy.api.v2.servant.model.signatures import FuncSignature
from lzy.api.v2.servant.model.slot import Slot

T = TypeVar("T")  # pylint: disable=invalid-name


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
    env: Env
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
        return json.dumps(
            {
                # tried to serialize env as json and it didn't work,
                # so build dict here instead for env
                "env": env.as_dct(),
                "fuze": self.command,
                "provisioning": {"tags": [{"tag": tag} for tag in provisioning.tags()]}
                if provisioning
                else {},
                "slots": [slot.to_dict() for slot in self.slots],
                "description": self.description,
            },
            sort_keys=True,
            indent=3,
        )
