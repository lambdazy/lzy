
from lzy.model.slot import Media, Slot, Direction


class FileSlot(Slot):
    @property
    def media(self) -> Media:
        return Media.FILE


class InFileSlot(FileSlot):
    @property
    def direction(self) -> Direction:
        return Direction.INPUT


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
