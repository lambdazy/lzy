import base64
import os
import sys
from typing import Any
import time
from pathlib import Path
from typing import TypeVar, Type

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import lazy_proxy
from lzy.api.whiteboard.model import UUIDEntryIdGenerator
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from pure_protobuf.dataclasses_ import load  # type: ignore
from lzy.api.serializer.serializer import Serializer

T = TypeVar("T")  # pylint: disable=invalid-name


def load_arg(path: Path, inp_type: Type[T]) -> Any:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        serializer = Serializer()
        return serializer.deserialize_from_file(file, inp_type)


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance()
    if 'LOCAL_MODULES' in os.environ:
        sys.path.append(os.environ['LOCAL_MODULES'])

    serializer = Serializer()
    print("Loading function")
    func_s: FuncSignature = serializer.deserialize_from_byte_string(base64.b64decode(argv[0].encode("ascii")))
    print("Function loaded: " + func_s.name)
    # noinspection PyShadowingNames
    args = tuple(
        lazy_proxy(
            lambda name=name: load_arg(servant.mount() / func_s.name / name, inp_type),
            inp_type,
            {},
        )
        for name, inp_type in zip(func_s.param_names, func_s.input_types)
    )
    lazy_call = CallSignature(func_s, args)
    print(f"Loaded {len(args)} lazy args")

    print(f"Running {func_s.name}")
    op_ = LzyRemoteOp(servant, lazy_call, deployed=True, entry_id_generator=UUIDEntryIdGenerator(""),
                      snapshot_id="")
    result = op_.materialize()
    print(f"Result of execution {result}")

    result_path = servant.mount() / func_s.name / "return"
    print(f"Writing result to file {result_path}")
    with open(result_path, "wb") as out_handle:
        serializer.serialize_to_file(result, out_handle, func_s.output_type)
        out_handle.flush()
        os.fsync(out_handle.fileno())


if __name__ == "__main__":
    main()
