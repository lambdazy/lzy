from dataclasses import dataclass
from typing import List, TypeVar, Union

from lzy.api.v1.servant.model.slot import Slot, DataSchema


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
    data_scheme: DataSchema
    spec: Union[SnapshotChannelSpec, DirectChannelSpec]


@dataclass
class Binding:
    slot: Slot
    channel: Channel


Bindings = List[Binding]

T = TypeVar("T")
