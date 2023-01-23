import dataclasses
import os
import sys
import time
from logging import Logger
from typing import Any, Callable, Mapping, Sequence, Tuple, Type, Optional, cast, Dict

from serialzy.api import SerializerRegistry

from lzy.api.v1.utils.pickle import unpickle
from lzy.api.v1.utils.types import infer_real_types
from lzy.logs.config import configure_logging, get_remote_logger
from lzy.proxy import proxy
from lzy.serialization.registry import LzySerializerRegistry, SerializerImport

NAME = __name__
_lzy_mount: Optional[str] = None  # for tests only
__lzy_lazy_argument = "__lzy_lazy_argument__"


def read_data(path: str, typ: Type, serializers: SerializerRegistry, logger: Logger) -> Any:
    ser = serializers.find_serializer_by_type(typ)

    name = path.split('/')[-1]
    logger.info(f"Reading {name} with serializer {type(ser).__name__}")

    mount = os.getenv("LZY_MOUNT", _lzy_mount)
    assert mount is not None

    with open(mount + path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        data = ser.deserialize(file, typ)  # type: ignore
        return data


def write_data(path: str, typ: Type, data: Any, serializers: SerializerRegistry, logger: Logger):
    mount = os.getenv("LZY_MOUNT", _lzy_mount)
    assert mount is not None

    ser = serializers.find_serializer_by_type(typ)
    if ser is None:
        raise ValueError(f'Cannot find serializer for type {typ}')
    if not ser.available():
        raise ValueError(
            f'Serializer for type {typ} is not available, please install {ser.requirements()}')

    if hasattr(data, __lzy_lazy_argument):  # if input argument is a return value
        data = data.__lzy_origin__  # type: ignore

    name = path.split('/')[-1]
    logger.info(f"Writing {name} with serializer {type(ser)}")
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
    logger: Logger,
    lazy_arguments: bool
):
    logger.info("Reading arguments...")

    try:
        args = [
            proxy(lambda path=path, typ=typ: read_data(path, typ, serializers, logger),  # type: ignore
                  infer_real_types(typ), cls_attrs={__lzy_lazy_argument: True}) if lazy_arguments else read_data(
                path, typ, serializers, logger) for typ, path in args_paths]
        kwargs = {
            name: proxy(lambda path=path, typ=typ: read_data(path, typ, serializers, logger),  # type: ignore
                        infer_real_types(typ), cls_attrs={__lzy_lazy_argument: True}) if lazy_arguments else read_data(
                path, typ, serializers, logger)
            for name, (typ, path) in kwargs_paths.items()
        }
    except Exception as e:
        logger.error(f"Error while reading arguments: {e}")
        raise e

    logger.info(f"Executing operation '{op.__name__}'")
    start = time.time()
    try:
        res = op(*args, **kwargs)
    except Exception as e:
        logger.error(f"Execution completed with error {e} in {time.time() - start}")
        raise e
    logger.info(f"Execution completed in {time.time() - start} sec")

    logger.info("Writing results...")
    try:
        if len(output_paths) == 1:
            write_data(output_paths[0][1], output_paths[0][0], res, serializers, logger)
            return
        for out, data in zip(output_paths, res):
            write_data(out[1], out[0], data, serializers, logger)
    except Exception as e:
        logger.error("Error while writing result: {}", e)
        raise e


@dataclasses.dataclass
class ProcessingRequest:
    logger_config: Dict[str, Any]
    serializers: Sequence[SerializerImport]
    op: Callable
    args_paths: Sequence[Tuple[Type, str]]
    kwargs_paths: Mapping[str, Tuple[Type, str]]
    output_paths: Sequence[Tuple[Type, str]]
    lazy_arguments: bool = True


def main(arg: str):
    try:
        if "LOCAL_MODULES" in os.environ:
            sys.path.append(os.environ["LOCAL_MODULES"])
        req: ProcessingRequest = cast(ProcessingRequest, unpickle(arg))
    except Exception as e:
        sys.stderr.write(f"Error while unpickling request: {e}")
        sys.stderr.flush()
        raise e

    registry = LzySerializerRegistry()
    try:
        registry.load_imports(req.serializers)
    except Exception as e:
        sys.stderr.write(f"Error while loading serializers: {e}")
        sys.stderr.flush()
        raise e

    try:
        configure_logging(req.logger_config)
    except Exception as e:
        sys.stderr.write(f"Error while logging configuration: {e}")
        sys.stderr.flush()
        raise e

    logger = get_remote_logger(__name__)
    logger.info("Starting remote runtime...")
    logger.debug(f"Running with environment: {os.environ}")
    process_execution(registry, req.op, req.args_paths, req.kwargs_paths, req.output_paths, logger, req.lazy_arguments)
    logger.info("Finishing remote runtime...")


if __name__ == "__main__":
    main(sys.argv[1])
