from lzy.proto.bet.priv.v2 import Slot, SlotDirection, SlotMedia, DataScheme


def create_slot(
    name: str,
    direction: SlotDirection,
    data_schema: DataScheme,
) -> Slot:
    return Slot(
        name=name,
        media=SlotMedia.FILE,
        direction=direction,
        content_type=data_schema,
    )


print(create_slot("test", SlotDirection.INPUT, None))
