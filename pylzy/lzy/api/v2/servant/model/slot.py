import base64
from pathlib import Path

import cloudpickle

from lzy.proto.bet.priv.v2 import SlotDirection, DataScheme, Slot, SlotMedia, SchemeType


def opposite(direction: SlotDirection):
    map_to_opposite = {
        SlotDirection.INPUT: SlotDirection.OUTPUT,
        SlotDirection.OUTPUT: SlotDirection.INPUT,
    }
    return map_to_opposite[direction]


def file_slot_t(
    name: Path,
    direction: SlotDirection,
    type_: type,
) -> Slot:
    return file_slot(name, direction, dump_type(type_))


def file_slot(
    name: Path,
    direction: SlotDirection,
    data_schema: DataScheme,
) -> Slot:
    return Slot(
        name=str(name),
        media=SlotMedia.FILE,
        direction=direction,
        content_type=data_schema,
    )


def pickle_type(type_: type) -> str:
    return base64.b64encode(cloudpickle.dumps(type_)).decode("ascii")


def unpickle_type(base64_str: str) -> type:
    return cloudpickle.loads(base64.b64decode(base64_str))


def dump_type(type_: type) -> DataScheme:
    return DataScheme(
        type=pickle_type(type_),
        scheme_type=SchemeType.cloudpickle,
    )
