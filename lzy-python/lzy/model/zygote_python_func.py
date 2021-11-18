import base64
import inspect
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Callable, Tuple, Type, TypeVar

import cloudpickle

from lzy.model.env import Env
from lzy.model.file_slots import create_slot
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote, Provisioning

T = TypeVar('T')


@dataclass
class FuncContainer:
    func: Callable
    input_types: Tuple[type, ...]
    output_type: Type[T]


@dataclass
class FuncContainer:
    func: Callable
    input_types: Tuple[type, ...]
    output_type: Type[T]


class ZygotePythonFunc(Zygote):
    def __init__(self, func: Callable, arg_types: Tuple[type, ...],
                 output_type: Type[T], lzy_mount: Path, env: Env, provisioning: Provisioning):
        super().__init__()
        self._func = func
        self._arg_types = arg_types
        self._return_type = output_type
        self._lzy_mount = lzy_mount
        self._env = env
        self._provisioning = provisioning

        self._arg_slots = []
        for arg_name in inspect.getfullargspec(func).args:
            slot = create_slot(os.path.join(os.sep, func.__name__, arg_name), Direction.INPUT)
            self._arg_slots.append(slot)
        self._return_slot = create_slot(os.path.join("/", func.__name__, "return"), Direction.OUTPUT)

    def name(self) -> str:
        return self._func.__name__

    def command(self) -> str:
        serialized_func = base64.b64encode(
            cloudpickle.dumps(FuncContainer(self._func, self._arg_types, self._return_type))).decode('ascii')
        return "python $(python -c 'import site; print(site.getsitepackages()[0])')/lzy/startup.py " + serialized_func

    def slots(self) -> List[Slot]:
        return self._arg_slots + [self._return_slot]

    def arg_slots(self) -> List[Slot]:
        return list(self._arg_slots)

    def return_slot(self) -> Slot:
        return self._return_slot

    def env(self) -> Env:
        return self._env

    def provisioning(self) -> Provisioning:
        return self._provisioning
