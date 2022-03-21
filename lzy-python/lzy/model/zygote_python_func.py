import base64
import os
from typing import Generic, TypeVar, Optional, Dict, List

from lzy.model.channel import Binding, Channel

from lzy.model.signatures import FuncSignature
from lzy.model.env import Env
from lzy.model.slot import Direction, Slot
from lzy.model.file_slots import create_slot
from lzy.model.zygote import Zygote, Provisioning
from lzy.api.serializer.serializer import Serializer
from lzy.servant.servant_client import ExecutionDescription

T = TypeVar("T")  # pylint: disable=invalid-name


class ZygotePythonFunc(Zygote, Generic[T]):
    def __init__(
        self,
        sign: FuncSignature[T],
        env: Env,
        provisioning: Optional[Provisioning],
        execution: Optional[ExecutionDescription] = None
    ):
        arg_slots: List[Slot] = []
        self._name_to_slot: Dict[str, Slot] = {}
        self.execution_description = execution

        for name in sign.param_names:
            slot = create_slot(os.path.join(os.sep, sign.name, name), Direction.INPUT)
            self._name_to_slot[name] = slot
            arg_slots.append(slot)

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
        serialized_func = base64.b64encode(Serializer.serialize_to_byte_string(self.signature)).decode('ascii')
        serialized_execution_description = base64.b64encode(
            Serializer.serialize_to_byte_string(self.execution_description)).decode('ascii')
        return _com + serialized_func + " " + serialized_execution_description

    def slot(self, arg: str) -> Slot:
        return self._name_to_slot[arg]
