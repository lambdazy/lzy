from abc import ABC
from dataclasses import dataclass

from lzy.model.slot import Media, Slot, Direction


@dataclass(frozen=True)
class FileSlot(Slot, ABC):
    @property
    def media(self) -> Media:
        return Media.FILE


@dataclass(frozen=True)
class InFileSlot(FileSlot):
    @property
    def direction(self) -> Direction:
        return Direction.INPUT


@dataclass(frozen=True)
class OutFileSlot(FileSlot):
    @property
    def direction(self) -> Direction:
        return Direction.OUTPUT


def create_slot(name: str, direction: Direction) -> Slot:
    if direction == Direction.INPUT:
        return InFileSlot(name)
    elif direction == Direction.OUTPUT:
        return OutFileSlot(name)
    else:
        raise ValueError(f"Cannot create fileslot for direction: {direction}")
