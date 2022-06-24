from lzy.api.v1.servant.model.slot import DataSchema, Direction, Media, Slot


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


def create_slot(name: str, direction: Direction, content_type: DataSchema) -> Slot:
    if direction == Direction.INPUT:
        return InFileSlot(name, content_type)

    if direction == Direction.OUTPUT:
        return OutFileSlot(name, content_type)

    raise ValueError(f"Cannot create fileslot for direction: {direction}")
