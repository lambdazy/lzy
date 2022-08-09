import hashlib
import inspect
import os
import uuid
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    get_type_hints,
)
from zipfile import ZipFile

# noinspection PyProtectedMember
from lzy._proxy import proxy
from lzy._proxy.result import Just, Nothing, Result
from lzy.api.v2.proxy_adapter import is_lzy_proxy
from lzy.api.v2.signatures import CallSignature, FuncSignature

if TYPE_CHECKING:
    from lzy.api.v2 import LzyCall, LzyWorkflow

T = TypeVar("T")  # pylint: disable=invalid-name

TypeInferResult = Result[type]


def infer_real_type(type_: Type) -> Type:
    if hasattr(type_, "__origin__"):
        origin: Type = type_.__origin__
        if origin == Union:  # type: ignore
            # noinspection PyUnresolvedReferences
            args = type_.__args__  # TODO: what should we do with real Union?
            if len(args) == 2 and args[1] is type(None):  # check typ is Optional
                return infer_real_type(args[0])
        return origin
    return type_


def infer_return_type(func: Callable) -> TypeInferResult:
    hints = get_type_hints(func)
    if "return" not in hints:
        return Nothing()

    or_type = hints["return"]
    or_type = infer_real_type(or_type)
    if isinstance(or_type, type):
        return Just(or_type)

    return Nothing()


def check_message_field(obj: Any) -> bool:
    if obj is None:
        return False
    return hasattr(obj, "LZY_MESSAGE")


def zipdir(path: str, zipfile: ZipFile):
    for root, dirs, files in os.walk(path):
        for file in files:
            zipfile.write(Path(root) / file, Path("../..") / root / file)


def fileobj_hash(fileobj: BytesIO) -> str:
    buf_size = 65_536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


def infer_call_signature(
    f: Callable, output_type: type, *args, **kwargs
) -> CallSignature:
    types_mapping = {}
    argspec = inspect.getfullargspec(f)

    # pylint: disable=protected-access
    for name, arg in chain(zip(argspec.args, args), kwargs.items()):
        # noinspection PyProtectedMember
        types_mapping[name] = (
            arg.lzy_call._op.output_type if is_lzy_proxy(arg) else type(arg)
        )

    generated_names = []
    for arg in args[len(argspec.args) :]:
        name = str(uuid.uuid4())
        generated_names.append(name)
        # noinspection PyProtectedMember
        types_mapping[name] = (
            arg.lzy_call._op.output_type if is_lzy_proxy(arg) else type(arg)
        )

    arg_names = tuple(argspec.args[: len(args)] + generated_names)
    kwarg_names = tuple(kwargs.keys())
    return CallSignature(
        FuncSignature(f, types_mapping, output_type, arg_names, kwarg_names),
        args,
        kwargs,
    )


class LzyExecutionException(Exception):
    def __init__(self, message, *args):
        message += (
            "If you are going to ask for help of cloud support,"
            " please send the following trace files: /tmp/lzy-log/"
        )
        super().__init__(message, *args)


def unwrap(val: Optional[T]) -> T:
    assert val is not None, "val is None :c"
    return cast(T, val)
