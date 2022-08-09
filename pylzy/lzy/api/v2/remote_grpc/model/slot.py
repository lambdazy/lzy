import base64
from pathlib import Path
from typing import cast

import cloudpickle

from ai.lzy.v1.zygote_pb2 import _DATASCHEME  # type: ignore
from ai.lzy.v1.zygote_pb2 import _SLOT_DIRECTION  # type: ignore
from ai.lzy.v1.zygote_pb2 import _SLOT_MEDIA  # type: ignore
from ai.lzy.v1.zygote_pb2 import DataScheme, Slot
from lzy.api.v2.remote_grpc.model._pickle import pickle


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


def dump_type(type_: type) -> DataScheme:
    return DataScheme(
        type=pickle(type_),
        schemeType=_DATASCHEME.cloudpickle,
    )
