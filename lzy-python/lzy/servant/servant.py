import abc
from pathlib import Path

from lzy.model.channel import Channel, Bindings, Binding
from lzy.model.file_slots import create_slot
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote


def _get_opposite_direction(direction: Direction):
    return {
        Direction.INPUT: Direction.OUTPUT,
        Direction.OUTPUT: Direction.INPUT
    }[direction]


class Servant:
    @abc.abstractmethod
    def mount(self) -> Path:
        pass

    @abc.abstractmethod
    def get_slot_path(self, slot: Slot) -> Path:
        pass

    @abc.abstractmethod
    def create_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def destroy_channel(self, channel: Channel):
        pass

    @abc.abstractmethod
    def touch(self, slot: Slot, channel: Channel):
        pass

    @abc.abstractmethod
    def publish(self, zygote: Zygote):
        pass

    @abc.abstractmethod
    def run(self, zygote: Zygote, bindings: Bindings):
        pass

    def configure_slots(self, zygote: Zygote, execution_id: str) -> Bindings:
        bindings = []
        for slot in zygote.slots():
            slot_full_name = "/task/" + execution_id + slot.name()
            local_slot = create_slot(slot_full_name, _get_opposite_direction(slot.direction()))
            channel = Channel(':'.join([execution_id, slot.name()]))
            self.create_channel(channel)
            self.touch(local_slot, channel)
            bindings.append(Binding(local_slot, slot, channel))
        return Bindings(bindings)
