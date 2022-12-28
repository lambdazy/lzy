import dataclasses
import os
import sys
import time
from typing import Any, Callable, Mapping, Sequence, Tuple, Type, Optional, cast

from serialzy.api import SerializerRegistry

from lzy.api.v1.utils.pickle import unpickle
from lzy.logging import get_remote_logger

_lzy_mount: Optional[str] = None  # for tests only

_LOG = get_remote_logger(__name__)


def read_data(path: str, typ: Type, serializers: SerializerRegistry) -> Any:
    ser = serializers.find_serializer_by_type(typ)

    _LOG.info(f"Reading data from {path} with type {typ} and serializer {type(ser)}")

    mount = os.getenv("LZY_MOUNT", _lzy_mount)
    assert mount is not None

    with open(mount + path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data = ser.deserialize(file, typ)  # type: ignore
        return data


def write_data(path: str, typ: Type, data: Any, serializers: SerializerRegistry):
    mount = os.getenv("LZY_MOUNT", _lzy_mount)
    assert mount is not None

    ser = serializers.find_serializer_by_type(typ)
    if ser is None:
        raise ValueError(f'Cannot find serializer for type {typ}')
    if not ser.available():
        raise ValueError(
            f'Serializer for type {typ} is not available, please install {ser.requirements()}')

    _LOG.info(f"Writing data to {path} with type {typ} and serializer {type(ser)}")
    with open(mount + path, "wb") as out_handle:
        out_handle.seek(0)
        out_handle.flush()
        ser.serialize(data, out_handle)
        out_handle.flush()
        os.fsync(out_handle.fileno())


def process_execution(
    serializers: SerializerRegistry,
    op: Callable,
    args_paths: Sequence[Tuple[Type, str]],
    kwargs_paths: Mapping[str, Tuple[Type, str]],
    output_paths: Sequence[Tuple[Type, str]],
):
    _LOG.info("Reading arguments...")

    try:
        args = [read_data(path, typ, serializers) for typ, path in args_paths]
        kwargs = {
            name: read_data(path, typ, serializers)
            for name, (typ, path) in kwargs_paths.items()
        }
    except Exception as e:
        _LOG.error(f"Error while reading arguments: {e}")
        raise e

    _LOG.info(f"Executing operation with args <{args}> and kwargs <{kwargs}>")
    try:
        res = op(*args, **kwargs)
    except Exception as e:
        _LOG.error(f"Exception while executing op: {e}")
        raise e

    _LOG.info("Writing results...")

    try:
        if len(output_paths) == 1:
            write_data(output_paths[0][1], output_paths[0][0], res, serializers)
            return
        for out, data in zip(output_paths, res):
            write_data(out[1], out[0], data, serializers)
    except Exception as e:
        _LOG.error("Error while writing result: {}", e)
        raise e

    _LOG.info("Execution completed")


@dataclasses.dataclass
class ProcessingRequest:
    serializers: SerializerRegistry
    op: Callable
    args_paths: Sequence[Tuple[Type, str]]
    kwargs_paths: Mapping[str, Tuple[Type, str]]
    output_paths: Sequence[Tuple[Type, str]]


def main(arg: str):
    if "LOCAL_MODULES" in os.environ:
        sys.path.append(os.environ["LOCAL_MODULES"])

    try:
        req: ProcessingRequest = cast(ProcessingRequest, unpickle(arg))
    except Exception as e:
        _LOG.error(f"Error while unpickling request: {e}")
        raise e

    process_execution(req.serializers, req.op, req.args_paths, req.kwargs_paths, req.output_paths)


if __name__ == "__main__":
    _LOG.info(f"Running with environment: {os.environ}")
    main(sys.argv[1])
