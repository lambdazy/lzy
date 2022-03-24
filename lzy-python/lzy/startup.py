import base64
import os
import sys
from typing import Optional
from typing import Any, Mapping
import time
from pathlib import Path
from pure_protobuf.dataclasses_ import load  # type: ignore
from typing import Any
from typing import TypeVar, Type

from lzy.api.hasher import hash_data
from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.serializer.serializer import Serializer
from lzy.api.utils import lazy_proxy
from lzy.api.whiteboard.model import UUIDEntryIdGenerator
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient, ExecutionDescription, InputExecutionValue
from pure_protobuf.dataclasses_ import load  # type: ignore
from lzy.api.serializer.serializer import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


def load_arg(path: Path, inp_type: Type[T], input_value: Optional[InputExecutionValue]) -> T:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data: T = Serializer.deserialize_from_file(file, inp_type)
        if input_value:
            input_value.hash = hash_data(data)
        return data


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance()
    if 'LOCAL_MODULES' in os.environ:
        sys.path.append(os.environ['LOCAL_MODULES'])

    print("Loading function")
    func_s: FuncSignature = Serializer.deserialize_from_byte_string(base64.b64decode(argv[0].encode("ascii")))
    exec_description: Optional[ExecutionDescription] = Serializer.deserialize_from_byte_string(base64.b64decode(argv[1].encode("ascii")))
    print("Function loaded: " + func_s.name)

    inputs: Mapping[str, InputExecutionValue] = {
        input_val.name: input_val
        for input_val in exec_description.inputs
    } if exec_description is not None else {}

    def build_proxy(arg_name: str) -> Any:
        return lazy_proxy(
            lambda n=arg_name: load_arg(servant.mount() / func_s.name / n, func_s.input_types[n], inputs.get(n)),  # type: ignore
            func_s.input_types[arg_name],
            {},
        )

    args = tuple(build_proxy(name) for name in func_s.arg_names)
    kwargs = {}
    for name in func_s.kwarg_names:
        kwargs[name] = build_proxy(name)

    lazy_call = CallSignature(func_s, args, kwargs)
    print(f"Loaded {len(args) + len(kwargs)} lazy args")

    print(f"Running {func_s.name}")
    op_ = LzyRemoteOp(servant, lazy_call, deployed=True, entry_id_generator=UUIDEntryIdGenerator(""),
                      snapshot_id="")
    result = op_.materialize()

    if exec_description is not None:
        servant.save_execution(exec_description)

    print(f"Result of execution {result}")

    result_path = servant.mount() / func_s.name / "return"
    print(f"Writing result to file {result_path}")
    with open(result_path, "wb") as out_handle:
        Serializer.serialize_to_file(result, out_handle, func_s.output_type)
        out_handle.flush()
        os.fsync(out_handle.fileno())


if __name__ == "__main__":
    main()
