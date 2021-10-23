import abc
import json
from typing import List

from lzy.model.slot import Slot
from lzy.model.env import Env


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

    # noinspection PyMethodMayBeStatic
    def provisioning(self) -> str:
        return "not implemented"

    def to_json(self) -> str:
        return json.dumps({
            # tried to serialize env as json and it didn't work,
            # so build dict here instead for env
            "env": {self.env().type_id(): self.env().as_dct()},
            "fuze": self.command(),
            "provisioning": self.provisioning(),
            "slots": [
                slot.to_dict() for slot in self.slots()
            ]
        }, sort_keys=True, indent=3)
