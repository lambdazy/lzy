import base64
import os
import sys
import time
from pathlib import Path
from typing import Any
from typing import Mapping
from typing import Optional
from typing import TypeVar, Type

from pure_protobuf.dataclasses_ import load  # type: ignore
from pure_protobuf.dataclasses_ import load  # type: ignore

from lzy.api.v1.lazy_op import LzyRemoteOp
from lzy.serialization.hasher import DelegatingHasher
from lzy.serialization.serializer import MemBytesSerializerImpl, FileSerializerImpl
from lzy.utils import lazy_proxy
from lzy.api.v1.whiteboard.model import UUIDEntryIdGenerator
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.model.execution import ExecutionDescription, InputExecutionValue

T = TypeVar("T")  # pylint: disable=invalid-name

mem_serializer = MemBytesSerializerImpl()
file_serializer = FileSerializerImpl()
hasher = DelegatingHasher(file_serializer)


def load_arg(path: Path, inp_type: Type[T], input_value: Optional[InputExecutionValue]) -> T:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data: T = file_serializer.deserialize(file, inp_type)
        if input_value:
            input_value.hash = hasher.hash(data)
        return data


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance()
    if 'LOCAL_MODULES' in os.environ:
        sys.path.append(os.environ['LOCAL_MODULES'])

    print("Loading function")
    func_s: FuncSignature = mem_serializer.deserialize(base64.b64decode(argv[0].encode("ascii")))
    exec_description: Optional[ExecutionDescription] = mem_serializer.deserialize(
        base64.b64decode(argv[1].encode("ascii")))
    print("Function loaded: " + func_s.name)

    inputs: Mapping[str, InputExecutionValue] = {
        input_val.name: input_val
        for input_val in exec_description.inputs
    } if exec_description is not None else {}

    def build_proxy(arg_name: str) -> Any:
        return lazy_proxy(
            lambda n=arg_name: load_arg(servant.mount() / func_s.name / n, func_s.input_types[n], inputs.get(n)), # type: ignore
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
    snapshot_id = "" if exec_description is None else exec_description.snapshot_id
    op_ = LzyRemoteOp(servant, lazy_call, snapshot_id,
                      UUIDEntryIdGenerator(snapshot_id),
                      mem_serializer,
                      file_serializer,
                      hasher,
                      deployed=True)
    result = op_.materialize()

    if exec_description is not None:
        servant.save_execution(exec_description)

    print(f"Result of execution {result}")

    result_path = servant.mount() / func_s.name / "return"
    print(f"Writing result to file {result_path}")
    with open(result_path, "wb") as out_handle:
        file_serializer.serialize(result, out_handle)
        out_handle.flush()
        os.fsync(out_handle.fileno())


if __name__ == "__main__":
    main()
