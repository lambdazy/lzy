from lzy.proto.bet.priv.v2 import SlotDirection


def opposite(direction: SlotDirection):
    map_to_opposite = {
        SlotDirection.INPUT: SlotDirection.OUTPUT,
        SlotDirection.OUTPUT: SlotDirection.INPUT,
    }
    return map_to_opposite[direction]
