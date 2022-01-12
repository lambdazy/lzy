import base64
import os
from pathlib import Path
from typing import Generic, List, TypeVar, Optional

import cloudpickle

from lzy.model.env import Env
from lzy.model.file_slots import create_slot
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote, Provisioning
from lzy.model.signatures import FuncSignature

T = TypeVar('T')


class ZygotePythonFunc(Zygote, Generic[T]):
    def __init__(self, func: FuncSignature[T], lzy_mount: Path,
                 env: Optional[Env], provisioning: Optional[Provisioning]):
        super().__init__()
        self._sign = func
        self._lzy_mount = lzy_mount
        self._env = env
        self._provisioning = provisioning

        self._arg_slots = []
        for arg_name in func.param_names:
            slot = create_slot(os.path.join(os.sep, func.name, arg_name), Direction.INPUT)
            self._arg_slots.append(slot)
        self._return_slot = create_slot(os.path.join("/", func.name, "return"), Direction.OUTPUT)

    def name(self) -> str:
        return self._sign.name

    def command(self) -> str:
        serialized_func = base64.b64encode(cloudpickle.dumps(self._sign)).decode('ascii')
        return "python $(python -c 'import site; print(site.getsitepackages()[0])')/lzy/startup.py " + serialized_func

    def slots(self) -> List[Slot]:
        return self._arg_slots + [self._return_slot]

    def arg_slots(self) -> List[Slot]:
        return list(self._arg_slots)

    def return_slot(self) -> Slot:
        return self._return_slot

    def env(self) -> Optional[Env]:
        return self._env

    def provisioning(self) -> Optional[Provisioning]:
        return self._provisioning
    
    def description(self) -> Optional[str]:
        return self._sign.description
