import base64
from pathlib import Path

import cloudpickle

from ai.lzy.v1.zygote_pb2 import (
    _DATASCHEME,
    _SLOT_DIRECTION,
    _SLOT_MEDIA,
    DataScheme,
    Slot,
)


def opposite(direction: _SLOT_DIRECTION):
    map_to_opposite = {
        _SLOT_DIRECTION.INPUT: _SLOT_DIRECTION.OUTPUT,
        _SLOT_DIRECTION.OUTPUT: _SLOT_DIRECTION.INPUT,
    }
    return map_to_opposite[direction]


def file_slot_t(
    name: Path,
    direction: _SLOT_DIRECTION,
    type_: type,
) -> Slot:
    return file_slot(name, direction, dump_type(type_))


def file_slot(
    name: Path,
    direction: _SLOT_DIRECTION,
    data_schema: _DATASCHEME,
) -> Slot:
    return Slot(
        name=str(name),
        media=_SLOT_MEDIA.FILE,
        direction=direction,
        contentType=data_schema,
    )


def pickle_type(type_: type) -> str:
    return base64.b64encode(cloudpickle.dumps(type_)).decode("ascii")


def unpickle_type(base64_str: str) -> type:
    return cloudpickle.loads(base64.b64decode(base64_str))


def dump_type(type_: type) -> DataScheme:
    return DataScheme(
        type=pickle_type(type_),
        schemeType=_DATASCHEME.cloudpickle,
    )
