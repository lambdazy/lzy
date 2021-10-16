from lzy.model.slot import Slot, DataSchema, Direction, Media


class InFileSlot(Slot):
    def __init__(self, name):
        super().__init__()
        self._name = name

    def name(self) -> str:
        return self._name

    def media(self) -> Media:
        return Media.FILE

    def direction(self) -> Direction:
        return Direction.INPUT

    def content_type(self) -> DataSchema:
        return DataSchema()


class OutFileSlot(Slot):
    def __init__(self, name):
        super().__init__()
        self._name = name

    def name(self) -> str:
        return self._name

    def media(self) -> Media:
        return Media.FILE

    def direction(self) -> Direction:
        return Direction.OUTPUT

    def content_type(self) -> DataSchema:
        return DataSchema()


def create_slot(name: str, direction: Direction) -> Slot:
    return {
        Direction.INPUT: InFileSlot(name),
        Direction.OUTPUT: OutFileSlot(name)
    }[direction]
