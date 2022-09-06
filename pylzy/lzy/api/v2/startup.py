import dataclasses
import os
import sys
import time
from typing import Callable, Mapping, Sequence, Any, Type, Tuple

from lzy.api.v2.utils._pickle import unpickle
from lzy.serialization.api import SerializersRegistry


def read_data(path: str, typ: Type, serializers: SerializersRegistry) -> Any:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data = serializers.find_serializer_by_type(typ).deserialize(file, typ)  # type: ignore
        return data


def write_data(path: str, data: Any, serializers: SerializersRegistry):
    typ = type(data)
    with open(path, "wb") as out_handle:
        serializers.find_serializer_by_type(typ).serialize(
            data, out_handle
        )
        out_handle.flush()
        os.fsync(out_handle.fileno())


def process_execution(
        serializers: SerializersRegistry,
        op: Callable,
        args_paths: Sequence[Tuple[Type, str]],
        kwargs_paths: Mapping[str, Tuple[Type, str]],
        output_paths: Sequence[str]
):
    print("Reading arguments...")
    args = [read_data(path, typ, serializers) for typ, path in args_paths]
    kwargs = {name: read_data(path, typ, serializers) for name, (typ, path) in kwargs_paths.items()}

    print("Executing operation")
    try:
        res = op(*args, **kwargs)
    except Exception as e:
        print("Exception while executing op:")
        raise e

    print("Writing arguments...")
    if len(output_paths) == 1:
        write_data(output_paths[0], res, serializers)
        return
    for path, data in zip(output_paths, res):
        write_data(path, data, serializers)
    print("Execution completed")


@dataclasses.dataclass
class ProcessingRequest:
    serializers: SerializersRegistry
    op: Callable
    args_paths: Sequence[Tuple[Type, str]]
    kwargs_paths: Mapping[str, Tuple[Type, str]]
    output_paths: Sequence[str]


def main(arg: str):
    req = unpickle(arg, ProcessingRequest)
    process_execution(req.serializers, req.op, req.args_paths, req.kwargs_paths, req.output_paths)


if __name__ == "__main__":
    main(sys.argv[1])
