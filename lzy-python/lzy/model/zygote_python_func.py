import base64
import os
from typing import Generic, TypeVar, Optional

import cloudpickle

from lzy.model.signatures import FuncSignature
from lzy.model.env import Env
from lzy.model.slot import Direction
from lzy.model.file_slots import create_slot
from lzy.model.zygote import Zygote, Provisioning

T = TypeVar("T")  # pylint: disable=invalid-name


class ZygotePythonFunc(Zygote, Generic[T]):
    def __init__(
        self,
        sign: FuncSignature[T],
        # lzy_mount: Path,
        env: Optional[Env],
        provisioning: Optional[Provisioning],
    ):
        # TODO: find out if lzy_mount is really needed here
        arg_slots = [
            create_slot(os.path.join(os.sep, sign.name, name), Direction.INPUT)
            for name in sign.param_names
        ]
        return_slot = create_slot(
            os.path.join("/", sign.name, "return"), Direction.OUTPUT
        )
        super().__init__(sign, arg_slots, return_slot, env, provisioning)

    # just a: /
    @property
    def command(self) -> str:
        _com = "".join([
            "python ",
            "$(python -c 'import site; print(site.getsitepackages()[0])')",
            "/lzy/startup.py "
        ])
        serialized_func = base64.b64encode(
            cloudpickle.dumps(self.signature)).decode('ascii')
        return _com + serialized_func
