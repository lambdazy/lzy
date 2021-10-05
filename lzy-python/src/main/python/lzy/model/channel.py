from dataclasses import dataclass
from typing import List

from lzy.model.slot import Slot, Direction


@dataclass
class Channel:
    name: str


@dataclass
class Binding:
    local_slot: Slot
    remote_slot: Slot
    channel: Channel


@dataclass
class Bindings:
    bindings: List[Binding]

    def local_slots(self, direction: Direction) -> List[Slot]:
        return list(
            filter(lambda x: x.direction() == direction,
                   map(lambda x: x.local_slot,
                       self.bindings))
        )
