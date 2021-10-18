import abc
import json
from enum import Enum


class Media(Enum):
    FILE = 'FILE'
    PIPE = 'PIPE'
    ARG = 'ARG'

    def to_json(self) -> str:
        return self.value


class Direction(Enum):
    INPUT = 'INPUT'
    OUTPUT = 'OUTPUT'

    def to_json(self) -> str:
        return self.value

    @staticmethod
    def opposite(direction):
        return {
            Direction.INPUT: Direction.OUTPUT,
            Direction.OUTPUT: Direction.INPUT
        }[direction]


class DataSchema:
    # noinspection PyMethodMayBeStatic
    def to_json(self) -> str:
        return "not implemented yet"


class Slot:
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def media(self) -> Media:
        pass

    @abc.abstractmethod
    def direction(self) -> Direction:
        pass

    @abc.abstractmethod
    def content_type(self) -> DataSchema:
        pass

    def __hash__(self):
        return hash(self.name())

    def __eq__(self, other):
        if isinstance(other, Slot):
            return self.name() == other.name()
        return False

    def to_dict(self):
        return {
            "name": self.name(),
            "media": self.media().to_json(),
            "direction": self.direction().to_json(),
            "contentType": self.content_type().to_json()
        }

    def to_json(self):
        return json.dumps(
            {
                # "name": "",  # FIXME: this is for touch
                "media": self.media().to_json(),
                "direction": self.direction().to_json(),
                "contentType": self.content_type().to_json()
            },
            sort_keys=True,
            indent=3
        )
