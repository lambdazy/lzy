import base64
from typing import List, Optional, TypeVar, Tuple
from pathlib import Path

from lzy.api.v2.servant.model.signatures import FuncSignature
from lzy.serialization.serializer import MemBytesSerializer

T = TypeVar("T")  # pylint: disable=invalid-name


from lzy.api.v2.servant.model.slot import file_slot_t

from ai.lzy.v1.zygote_pb2 import EnvSpec, Provisioning, Slot, _SLOT_DIRECTION, Zygote
from ai.lzy.v1.whiteboard_pb2 import ExecutionDescription


def send_local_slots_to_s3(signature: FuncSignature[T]) -> Tuple[List[Slot], Slot]:
    arg_slots: List[Slot] = [
        file_slot_t(Path(signature.name) / name, _SLOT_DIRECTION.INPUT, type_)
        for name, type_ in signature.input_types.items()
    ]

    return_slot: Slot = file_slot_t(
        Path("fnc_name") / "return", _SLOT_DIRECTION.OUTPUT, signature.output_type
    )
    return arg_slots, return_slot


def to_base64(inp: bytes) -> str:
    return base64.b64encode(inp).decode("ascii")


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
    serialized_func = to_base64(serializer.serialize_to_string(signature))
    serialized_execution_description = to_base64(
        serializer.serialize_to_string(execution)
    )
    return _com + serialized_func + " " + serialized_execution_description


def python_func_zygote(
    serializer: MemBytesSerializer,
    sign: FuncSignature[T],
    env: EnvSpec,
    provisioning: Provisioning = "",
    execution: Optional[ExecutionDescription] = None,
) -> Zygote:
    fuze = generate_fuze(sign, serializer, execution)
    # TODO[ottergottaott]: Create slots properly
    # arg_slots, return_slot = create_slots(sign)
    raise NotImplementedError("")
    arg_slots, return_slot = (), None
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
