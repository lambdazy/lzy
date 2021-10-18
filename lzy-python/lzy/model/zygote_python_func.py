import base64
from pathlib import Path
from typing import List, Callable

import cloudpickle

from lzy.model.slot import Slot
from lzy.model.zygote import Zygote


class ZygotePythonFunc(Zygote):
    def __init__(self, func: Callable, arg_slots: List[Slot], return_slot: Slot, lzy_mount: Path):
        super().__init__()
        self._func = func
        self._arg_slots = list(arg_slots)
        self._return_slot = return_slot
        self._lzy_mount = lzy_mount

    def name(self) -> str:
        return self._func.__name__

    def command(self) -> str:
        serialized_func = base64.b64encode(cloudpickle.dumps(self._func)).decode('ascii')
        return f"python3 /lzy-python/lzy/startup.py " + serialized_func

    def slots(self) -> List[Slot]:
        return self._arg_slots + [self._return_slot]

    def arg_slots(self) -> List[Slot]:
        return list(self._arg_slots)

    def return_slot(self) -> Slot:
        return self._return_slot
