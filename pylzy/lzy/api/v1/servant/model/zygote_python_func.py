import base64
import os
from typing import Dict, Generic, List, Optional, TypeVar

from lzy.api.v1.servant.model.env import Env
from lzy.api.v1.servant.model.execution import ExecutionDescription
from lzy.api.v1.servant.model.file_slots import create_slot
from lzy.api.v1.servant.model.slot import DataSchema, Direction, Slot, pickle_type
from lzy.api.v1.servant.model.zygote import Provisioning, Zygote
from lzy.api.v1.signatures import FuncSignature
from lzy.serialization.serializer import MemBytesSerializer

T = TypeVar("T")  # pylint: disable=invalid-name


class ZygotePythonFunc(Zygote, Generic[T]):
    def __init__(
        self,
        serializer: MemBytesSerializer,
        sign: FuncSignature[T],
        env: Env,
        provisioning: Optional[Provisioning],
        execution: Optional[ExecutionDescription] = None,
    ):
        arg_slots: List[Slot] = []
        self._serializer = serializer
        self._name_to_slot: Dict[str, Slot] = {}
        self.execution_description = execution

        for name, type_ in sign.input_types.items():
            slot = create_slot(
                os.path.join(os.sep, sign.name, name),
                Direction.INPUT,
                DataSchema.generate_schema(type_),
            )
            self._name_to_slot[name] = slot
            arg_slots.append(slot)

        return_slots = []
        for num, type_ in enumerate(sign.output_types):
            return_slot = create_slot(
                os.path.join("/", sign.name, "return", str(num)),
                Direction.OUTPUT,
                DataSchema.generate_schema(type_),
            )
            return_slots.append(return_slot)
        super().__init__(sign, arg_slots, return_slots, env, provisioning)

    # just a: /
    @property
    def command(self) -> str:
        _com = "".join(
            [
                "python ",
                "$(python -c 'import site; print(site.getsitepackages()[0])')",
                "/lzy/api/v1/startup.py ",
            ]
        )
        serialized_func = base64.b64encode(
            self._serializer.serialize_to_string(self.signature)
        ).decode("ascii")
        serialized_execution_description = base64.b64encode(
            self._serializer.serialize_to_string(self.execution_description)
        ).decode("ascii")
        return _com + serialized_func + " " + serialized_execution_description

    def slot(self, arg: str) -> Slot:
        return self._name_to_slot[arg]
