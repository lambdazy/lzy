from dataclasses import dataclass
from typing import List, Union, TypeVar

from lzy.api.v2.servant.model.slot import Slot


@dataclass
class SnapshotChannelSpec:
    snapshot_id: str
    entry_id: str


@dataclass
class DirectChannelSpec:
    pass


@dataclass
class Channel:
    name: str
    spec: Union[SnapshotChannelSpec, DirectChannelSpec]


@dataclass
class Binding:
    slot: Slot
    channel: Channel


Bindings = List[Binding]

T = TypeVar("T")
