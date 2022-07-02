import base64
import os
from typing import Dict, Generic, List, Optional, TypeVar, Iterable
from pathlib import Path

from lzy.api.v2.servant.model.signatures import FuncSignature
from lzy.serialization.serializer import MemBytesSerializer

T = TypeVar("T")  # pylint: disable=invalid-name


from lzy.api.v2.servant.model.slot import file_slot, dump_type
from lzy.proto.bet.priv.v2 import Zygote, Slot, SlotDirection, Provisioning, EnvSpec, ExecutionDescription


def create_slots(signature: FuncSignature[T]):
    arg_slots: List[Slot] = [
        file_slot(
            Path(signature.name) / name,
            SlotDirection.INPUT,
            dump_type(type_)
        )
        for name, type_ in signature.input_types.items()
    ]

    return_slot = file_slot(
        Path("fnc_name") / "return",
        SlotDirection.OUTPUT,
        dump_type(signature.output_type)
    )
    return arg_slots, return_slot


def generate_fuze(
    signature: FuncSignature[T],
    serializer: MemBytesSerializer,
    execution: Optional[ExecutionDescription] = None,
) -> str:
    _com = "".join(
        [
            "python ",
            "$(python -c 'import site; print(site.getsitepackages()[0])')",
            "/lzy/api/v1/startup.py ",
        ]
    )
    serialized_func = base64.b64encode(
        serializer.serialize_to_string(signature)
    ).decode("ascii")
    serialized_execution_description = base64.b64encode(
        serializer.serialize_to_string(execution)
    ).decode("ascii")
    return _com + serialized_func + " " + serialized_execution_description


def python_func_zygote(
    serializer: MemBytesSerializer,
    sign: FuncSignature[T],
    env: EnvSpec,
    provisioning: Provisioning = "",
    execution: Optional[ExecutionDescription] = None,
) -> Zygote:
    fuze = generate_fuze(sign, serializer, execution)
    arg_slots, return_slot = create_slots(sign)
    return Zygote(
        env=env,
        provisioning=provisioning,
        fuze=fuze,
        slots=[
            *arg_slots,
            return_slot,
        ],
        description=sign.description,
        name=sign.name,
    )
