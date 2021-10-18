from dataclasses import dataclass
from typing import List, Optional

from lzy.model.slot import Slot


@dataclass
class Channel:
    name: str


@dataclass
class Binding:
    local_slot: Slot
    remote_slot: Slot
    channel: Channel


class Bindings:
    def __init__(self, bindings: List[Binding]):
        self._bindings = list(bindings)
        self._to_local_map = dict()
        for bind in bindings:
            self._to_local_map[bind.remote_slot] = bind.local_slot

    def bindings(self) -> List[Binding]:
        return list(self._bindings)

    def local_slot(self, remote_slot: Slot) -> Optional[Slot]:
        return self._to_local_map.get(remote_slot, None)
