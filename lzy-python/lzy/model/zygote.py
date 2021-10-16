import abc
import json
from typing import List

from lzy.model.slot import Slot


class Zygote:
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def command(self) -> str:
        pass

    @abc.abstractmethod
    def slots(self) -> List[Slot]:
        pass

    # noinspection PyMethodMayBeStatic
    def provisioning(self) -> str:
        return "not implemented"

    def to_json(self) -> str:
        return json.dumps(
            {
                "fuze": self.command(),
                "provisioning": self.provisioning(),
                "slots": [
                    slot.to_dict() for slot in self.slots()
                ]
            },
            sort_keys=True,
            indent=3
        )
