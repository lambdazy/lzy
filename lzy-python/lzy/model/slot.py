from dataclasses import dataclass
from abc import ABC, abstractmethod
import json
from enum import Enum


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


class DataSchema:
    # noinspection PyMethodMayBeStatic
    # pylint: disable=no-self-use
    def to_json(self) -> str:
        return "not implemented yet"


# actually Slot should be just marked as
# @dataclass(frozen=True)
# but mypy is broken here a bit, so workaround with mixin is needed:
# https://stackoverflow.com/questions/69330256/how-to-get-an-abstract-dataclass-to-pass-mypy
# https://github.com/python/mypy/issues/5374#issuecomment-568335302
@dataclass(frozen=True)
class SlotDataclassMixin:
    name: str


class Slot(SlotDataclassMixin, ABC):
    @property
    @abstractmethod
    def direction(self) -> Direction:
        pass

    @property
    @abstractmethod
    def media(self) -> Media:
        pass

    @property
    def content_type(self) -> DataSchema:
        return DataSchema()

    def to_dict(self):
        return {
            "name": self.name,
            "media": self.media.to_json(),
            "direction": self.direction.to_json(),
            "contentType": self.content_type.to_json(),
        }

    def to_json(self):
        return json.dumps(
            {
                # "name": "",  # FIXME: this is for touch
                "media": self.media.to_json(),
                "direction": self.direction.to_json(),
                "contentType": self.content_type.to_json(),
            },
            sort_keys=True,
            indent=3,
        )
