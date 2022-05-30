from lzy.api.v2.servant.model.slot import Slot, Media, Direction


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

    if direction == Direction.OUTPUT:
        return OutFileSlot(name)

    raise ValueError(f"Cannot create fileslot for direction: {direction}")
