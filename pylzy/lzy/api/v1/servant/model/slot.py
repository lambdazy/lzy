import base64
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, cast, Any

import cloudpickle

from lzy.serialization.types import File


def pickle_type(type_: type) -> str:
    return base64.b64encode(cloudpickle.dumps(type_)).decode("ascii")


def unpickle_type(base64_str: str) -> type:
    type_ = cloudpickle.loads(base64.b64decode(base64_str))
    assert isinstance(type_, type), "is not type"
    return cast(type, type_)


class Media(Enum):
    FILE = "FILE"
    PIPE = "PIPE"
    ARG = "ARG"

    def to_json(self) -> str:
        return self.value


class Direction(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"

    def to_json(self) -> str:
        return self.value

    @staticmethod
    def opposite(direction):
        return {Direction.INPUT: Direction.OUTPUT, Direction.OUTPUT: Direction.INPUT}[
            direction
        ]


PLAINTEXT_SCHEME_TYPE = "plain"
FILE_TYPE = "file"

CLOUDPICKLE_SCHEME_TYPE = "cloudpickle"


@dataclass(frozen=True)
class DataSchema:
    # TODO(aleksZubakov): probably we should expand this for case when type defined as proto
    type_: str  # base64 encoded
    schemeType: str = "cloudpickle"

    @property
    def real_type(self) -> type:
        if self.schemeType == PLAINTEXT_SCHEME_TYPE:
            if self.type_ == FILE_TYPE:
                return File
            raise ValueError("Unsupported DataScheme type")
        if self.schemeType == CLOUDPICKLE_SCHEME_TYPE:
            return unpickle_type(self.type_)
        raise ValueError("Unsupported DataScheme type")

    def to_dict(self) -> Dict[str, str]:
        return {
            "type": self.type_,
            "schemeType": self.schemeType,
        }

    def to_json(self) -> str:
        # TODO(aleksZubakov): has it be here?
        return json.dumps(self.to_dict(), sort_keys=True)

    @staticmethod
    def generate_schema(typ: type) -> 'DataSchema':
        if typ == File:
            return DataSchema(FILE_TYPE, PLAINTEXT_SCHEME_TYPE)
        return DataSchema(pickle_type(typ), CLOUDPICKLE_SCHEME_TYPE)


# actually Slot should be just marked as
# @dataclass(frozen=True)
# but mypy is broken here a bit, so workaround with mixin is needed:
# https://stackoverflow.com/questions/69330256/how-to-get-an-abstract-dataclass-to-pass-mypy
# https://github.com/python/mypy/issues/5374#issuecomment-568335302
@dataclass(frozen=True)
class SlotDataclassMixin:
    name: str
    content_type: DataSchema


class Slot(SlotDataclassMixin, ABC):
    @property
    @abstractmethod
    def direction(self) -> Direction:
        pass

    @property
    @abstractmethod
    def media(self) -> Media:
        pass

    def to_dict(self):
        return {
            "name": self.name,
            "media": self.media.to_json(),
            "direction": self.direction.to_json(),
            "contentType": self.content_type.to_dict(),
        }

    def to_json(self):
        return json.dumps(
            {
                # "name": "",  # FIXME: this is for touch
                "media": self.media.to_json(),
                "direction": self.direction.to_json(),
                "contentType": self.content_type.to_dict(),
            },
            sort_keys=True,
            indent=3,
        )
