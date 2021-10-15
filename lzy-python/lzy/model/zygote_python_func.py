import os
from pathlib import Path
from typing import Callable, List

from lzy.model.file_slots import create_slot
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote


class ZygotePythonFunc(Zygote):
    def __init__(self, func: Callable, lzy_mount: Path):
        super().__init__()
        self._func = func
        self._name = self._func.__name__
        self.lzy_mount = lzy_mount

        self.input_slot = create_slot(os.path.join("/", self._name, "input"), Direction.INPUT)
        self.output_slot = create_slot(os.path.join("/", self._name, "output"), Direction.OUTPUT)

    def name(self) -> str:
        return self._name

    def command(self) -> str:
        return f"python3 " \
               f"/lzy-python/lzy/startup.py " + \
               str(self.lzy_mount.joinpath(self.input_slot.name().lstrip(os.sep))) + " " + \
               str(self.lzy_mount.joinpath(self.output_slot.name().lstrip(os.sep)))

    def slots(self) -> List[Slot]:
        return [self.input_slot, self.output_slot]
