import base64
import dataclasses
import datetime
import os
import sys
import time
from typing import Any, Callable, Mapping, Sequence, Tuple, Type, TypeVar, cast

from lzy.serialization.api import SerializerRegistry

import cloudpickle

T = TypeVar("T")


def unpickle(base64_str: str, obj_type: Type[T] = None) -> T:
    t = cloudpickle.loads(base64.b64decode(base64_str.encode("ascii")))
    return cast(T, t)


def read_data(path: str, typ: Type, serializers: SerializerRegistry) -> Any:
    ser = serializers.find_serializer_by_type(typ)

    log(f"Reading data from {path} with type {typ} and serializer {type(ser)}")

    mount = os.getenv("LZY_MOUNT")
    with open(mount + path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data = ser.deserialize(file, typ)  # type: ignore
        return data


def write_data(path: str, data: Any, serializers: SerializerRegistry):
    mount = os.getenv("LZY_MOUNT")
    typ = type(data)

    ser = serializers.find_serializer_by_type(typ)
    log(f"Writing data to {path} with type {typ} and serializer {type(ser)}")

    with open(mount + path, "wb") as out_handle:
        out_handle.seek(0)
        out_handle.flush()
        ser.serialize(data, out_handle)
        out_handle.flush()
        os.fsync(out_handle.fileno())


def log(msg: str, *args, **kwargs):
    now = datetime.datetime.utcnow()
    time_prefix = now.strftime("%Y-%m-%d %H:%M:%S")
    if args:
        print("[LZY]", time_prefix, msg.format(args, kwargs))
    else:
        print("[LZY]", time_prefix, msg)


def process_execution(
    serializers: SerializerRegistry,
    op: Callable,
    args_paths: Sequence[Tuple[Type, str]],
    kwargs_paths: Mapping[str, Tuple[Type, str]],
    output_paths: Sequence[str],
):
    log("Reading arguments...")
    args = [read_data(path, typ, serializers) for typ, path in args_paths]
    kwargs = {
        name: read_data(path, typ, serializers)
        for name, (typ, path) in kwargs_paths.items()
    }

    log(f"Executing operation with args <{args}> and kwargs <{kwargs}>")
    try:
        res = op(*args, **kwargs)
    except Exception as e:
        log("Exception while executing op:")
        raise e

    log("Writing arguments...")
    if len(output_paths) == 1:
        write_data(output_paths[0], res, serializers)
        return
    for path, data in zip(output_paths, res):
        write_data(path, data, serializers)
    log("Execution completed")


@dataclasses.dataclass
class ProcessingRequest:
    serializers: SerializerRegistry
    op: Callable
    args_paths: Sequence[Tuple[Type, str]]
    kwargs_paths: Mapping[str, Tuple[Type, str]]
    output_paths: Sequence[str]


def main(arg: str):
    req = unpickle(arg, ProcessingRequest)
    process_execution(
        req.serializers, req.op, req.args_paths, req.kwargs_paths, req.output_paths
    )


if __name__ == "__main__":
    log(f"Running with environment: {os.environ}")
    main(sys.argv[1])
