import os
import sys

# update sys path here to allow importing lzy from local modules
if "LOCAL_MODULES" in os.environ:
    sys.path.insert(0, os.environ["LOCAL_MODULES"])

import dataclasses
import time
from logging import Logger
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple, Type, Optional, cast, Dict

from serialzy.api import SerializerRegistry
from serialzy.types import get_type

from lzy.api.v1.utils.pickle import unpickle
from lzy.api.v1.utils.types import infer_real_types, check_types_serialization_compatible, is_subtype
from lzy.logs.config import configure_logging, get_remote_logger
from lzy.proxy import proxy
from lzy.serialization.registry import LzySerializerRegistry, SerializerImport

NAME = __name__
MAIN_PID_ENV_VAR = 'LZY_OP_MAIN_PID'
_lzy_mount: Optional[str] = None  # for tests only
__lzy_lazy_argument = "__lzy_lazy_argument__"

__read_cache: Dict[str, Any] = {}


def read_data(path: str, typ: Type, serializers: SerializerRegistry, logger: Logger) -> Any:
    if path in __read_cache:
        return __read_cache[path]

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
        data = ser.deserialize(file)  # type: ignore
        __read_cache[path] = data
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

    real_type = get_type(data)
    compatible = check_types_serialization_compatible(typ, real_type, serializers)
    if not compatible or not is_subtype(real_type, typ):
        raise TypeError(
            f"Invalid types: return value has type {typ} "
            f"but passed type {real_type}")

    name = path.split('/')[-1]
    logger.info(f"Writing {name} with serializer {type(ser)}")
    with open(mount + path, "wb") as out_handle:
        ser.serialize(data, out_handle)
        out_handle.flush()


def _get_main_pid() -> int:
    """
    Some programs can be replicated by subprocesses, i.e. as in default DDP strategy in Pytorch Lightning. This will
    cause repeated execution of this startup script. We write @op results only from initial process to avoid multiple
    writes, which will cause problems in output slots files.

    We consider different approaches to check if process is initial:
    - Setting environment variable LZY_OP_MAIN_PID value equal to PID of initial process. On start, each process
      match its PID with LZY_OP_MAIN_PID, if they are equal, than process is initial.
    - Creating pidfile. Initial process creates marker file, subprocesses will check its existence to understand
      that they are not initial.

    We choose usage of LZY_OP_MAIN_PID. Initial process set this variable by itself, counting on the fact that the
    subprocesses will inherit all environment variables from their parent. If subprocesses which don't inherit
    environment variables will become a common use-case, we will have to change way of setting LZY_OP_MAIN_PID.

    As alternative, we can set this variable in run command, such as "LZY_OP_MAIN_PID=$BASHPID python startup.py ...".
    However, this approach can work differently depending on the way of launching of startup script.

    As for pidfile, since this file cannot be guaranteed to be deleted (i.e. if execution crashed before deletion), we
    have to construct its path such as it cannot be clashed with further @op execution, if they are eventually will be
    scheduled to the same pod without cleanup (i.e., same lzy user of @op retry). It will require to include some
    # @op metadata (operation ID, launch ID, ...) to startup script input, what looks like an abstraction leak.
    """
    if MAIN_PID_ENV_VAR not in os.environ:
        os.environ[MAIN_PID_ENV_VAR] = str(os.getpid())
    return int(os.environ[MAIN_PID_ENV_VAR])


def process_execution(
    serializers: SerializerRegistry,
    op: Callable,
    args_paths: Sequence[Tuple[Type, str]],
    kwargs_paths: Mapping[str, Tuple[Type, str]],
    output_paths: Sequence[Tuple[Type, str]],
    exception_path: Tuple[Type, str],
    logger: Logger,
    lazy_arguments: bool
):
    pid, ppid = os.getpid(), os.getppid()
    main_pid = _get_main_pid()
    logger.debug("Starting process with pid %d (ppid %d, main pid %s)", pid, ppid, main_pid)

    logger.info("Reading arguments...")
    exc_typ, exc_path = exception_path

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
        write_data(exc_path, exc_typ, sys.exc_info(), serializers, logger)
        raise

    logger.info(f"Executing operation '{op.__name__}'")
    start = time.time()
    try:
        res = op(*args, **kwargs)
    except Exception as e:
        logger.error(f"Execution completed with error `{e}` in {time.time() - start}")
        write_data(exc_path, exc_typ, sys.exc_info(), serializers, logger)
        raise
    finally:
        # TODO: actually needed only for unit tests, move to common tests tearDown() logic
        del os.environ[MAIN_PID_ENV_VAR]
    logger.info(f"Execution completed in {time.time() - start} sec")

    if main_pid != pid:
        logger.debug("Don't write results from not main process with pid %d", pid)
        return

    logger.info("Writing results...")
    try:
        if len(output_paths) == 1:
            write_data(output_paths[0][1], output_paths[0][0], res, serializers, logger)
        else:
            for out, data in zip(output_paths, res):
                write_data(out[1], out[0], data, serializers, logger)
        write_data(exc_path, type(None), None, serializers, logger)
    except Exception as e:
        logger.error("Error while writing result: {}", e)
        write_data(exc_path, exc_typ, sys.exc_info(), serializers, logger)
        raise


@dataclasses.dataclass
class ProcessingRequest:
    logger_config: Dict[str, Any]
    serializers: Sequence[SerializerImport]
    op: Callable
    args_paths: Sequence[Tuple[Type, str]]
    kwargs_paths: Mapping[str, Tuple[Type, str]]
    output_paths: Sequence[Tuple[Type, str]]
    exception_path: Tuple[Type, str]
    lazy_arguments: bool = True


def main(arg: str):
    try:
        # remove current dir from sys path to avoid importing [local,remote,utils] and other internal modules
        parent_dir = str(Path(__file__).parent)
        for path in sys.path:
            if path == parent_dir:
                sys.path.remove(path)
    except Exception as e:
        sys.stderr.write(f"Error while preparing sys path: {e}")
        sys.stderr.flush()
        raise e

    try:
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
    logger.info("Starting execution...")
    logger.debug(f"Running with environment: {os.environ}")
    process_execution(registry, req.op, req.args_paths, req.kwargs_paths, req.output_paths,
                      req.exception_path, logger, req.lazy_arguments)
    logger.info("Finishing execution...")


if __name__ == "__main__":
    main(sys.argv[1])
