import base64
from dataclasses import dataclass
from abc import ABC, abstractmethod
import json
from enum import Enum
from typing import Dict

import cloudpickle


def pickle_type(type_: type) -> str:
    return base64.b64encode(cloudpickle.dumps(type_)).decode('ascii')


def unpickle_type(base64_str: str) -> type:
    return cloudpickle.loads(base64.b64decode(base64_str))


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


@dataclass(frozen=True)
class DataSchema:
    # TODO(aleksZubakov): probably we should expand this for case when type defined as proto
    type_: str  # base64 encoded
    schemeType: str = "cloudpickle"

    @property
    def real_type(self) -> type:
        return unpickle_type(self.type_)

    def to_dict(self) -> Dict[str, str]:
        return {
           "type": self.type_,
           "schemeType": self.schemeType,
        }

    def to_json(self) -> str:
        # TODO(aleksZubakov): has it be here?
        return json.dumps(self.to_dict(), sort_keys=True)


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
