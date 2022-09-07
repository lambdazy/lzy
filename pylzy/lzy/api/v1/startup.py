import datetime
import os
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Optional, Type, TypeVar

from lzy.api.v1 import LzyRemoteOp, UUIDEntryIdGenerator
from lzy.api.v1.servant.bash_servant_client import BashServantClient
from lzy.api.v1.servant.model.execution import ExecutionDescription, InputExecutionValue
from lzy.api.v1.servant.servant_client import ServantClient
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.utils import lazy_proxy, unpickle
from lzy.serialization.hasher import DelegatingHasher
from lzy.serialization.registry import DefaultSerializerRegistry

T = TypeVar("T")  # pylint: disable=invalid-name

file_serializer = DefaultSerializerRegistry()
hasher = DelegatingHasher(file_serializer)


def log(msg: str, *args, **kwargs):
    now = datetime.datetime.utcnow()
    time_prefix = now.strftime("%Y-%m-%d %H:%M:%S")
    if args:
        print("[LZY]", time_prefix, msg.format(args, kwargs))
    else:
        print("[LZY]", time_prefix, msg)


def load_arg(
    path: Path, inp_type: Type[T], input_value: Optional[InputExecutionValue]
) -> T:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data: T = file_serializer.find_serializer_by_type(inp_type).deserialize(
            file, inp_type
        )
        if input_value:
            input_value.hash = hasher.hash(data)
        return data


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance(os.getenv("LZY_MOUNT"))
    if "LOCAL_MODULES" in os.environ:
        sys.path.append(os.environ["LOCAL_MODULES"])

    log("Loading function")
    func_s: FuncSignature = unpickle(argv[0], FuncSignature)
    exec_description: Optional[ExecutionDescription] = unpickle(
        argv[1], ExecutionDescription
    )
    log("Function loaded: " + func_s.name)

    inputs: Mapping[str, InputExecutionValue] = (
        {input_val.name: input_val for input_val in exec_description.inputs}
        if exec_description is not None
        else {}
    )

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
    log(f"Loaded {len(args) + len(kwargs)} lazy args")

    log(f"Running {func_s.name}")
    snapshot_id = "" if exec_description is None else exec_description.snapshot_id
    op_ = LzyRemoteOp(
        servant,
        lazy_call,
        snapshot_id,
        UUIDEntryIdGenerator(snapshot_id),
        file_serializer,
        hasher,
        deployed=True,
    )

    for num, val in enumerate(op_.return_values()):

        result_path = servant.mount() / func_s.name / "return" / str(num)
        log(f"Writing result to file {result_path}")
        with open(result_path, "wb") as out_handle:
            materialize = val.materialize()
            file_serializer.find_serializer_by_type(val.type).serialize(
                materialize, out_handle
            )
            out_handle.flush()
            os.fsync(out_handle.fileno())
    log("Execution done")
    if exec_description is not None:
        servant.save_execution(exec_description)


if __name__ == "__main__":
    main()
