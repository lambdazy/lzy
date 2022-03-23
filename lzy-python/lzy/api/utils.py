from itertools import chain

import inspect

import os
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    Type,
    Tuple,
    TypeVar,
    get_type_hints,
)
import hashlib

from zipfile import ZipFile

# noinspection PyProtectedMember
from lzy.api._proxy import proxy
from lzy.api.result import Result, Just, Nothing
from lzy.model.signatures import CallSignature, FuncSignature

T = TypeVar("T")  # pylint: disable=invalid-name

TypeInferResult = Result[type]


def infer_return_type(func: Callable) -> TypeInferResult:
    hints = get_type_hints(func)
    if "return" not in hints:
        return Nothing()

    or_type = hints["return"]
    if hasattr(or_type, "__origin__"):
        return Just(or_type.__origin__)  # type: ignore

    if isinstance(or_type, type):
        return Just(or_type)

    return Nothing()


def infer_arg_types(*args) -> Tuple[type, ...]:
    # noinspection PyProtectedMember
    # pylint: disable=protected-access
    return tuple(
        arg._op.return_type
        if is_lazy_proxy(arg) else type(arg)
        for arg in args
    )


def is_lazy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, "__lzy_proxied__") and cls.__lzy_proxied__


def lazy_proxy(
        materialization: Callable[[], T], return_type: Type[T], obj_attrs: Dict[str, Any]
) -> Any:
    return proxy(
        materialization,
        return_type,
        cls_attrs={"__lzy_proxied__": True},
        obj_attrs=obj_attrs,
    )


def is_wrapped_local_value(obj: Any) -> bool:
    return hasattr(obj, "__lzy_local_value__") and obj.__lzy_local_value__


def wrap_local_value(obj: Any):
    obj.__lzy_local_value__ = True


def check_message_field(obj: Any) -> bool:
    if obj is None:
        return False
    return hasattr(obj, 'LZY_MESSAGE')


def zipdir(path: str, zipfile: ZipFile):
    for root, dirs, files in os.walk(path):
        for file in files:
            zipfile.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, '..'))
            )


def fileobj_hash(fileobj: BytesIO) -> str:
    buf_size = 65536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


def infer_call_signature(f: Callable, output_type: type, *args, **kwargs) -> CallSignature:
    types_mapping = {}
    argspec = inspect.getfullargspec(f)

    # pylint: disable=protected-access
    for name, arg in chain(zip(argspec.args, args), kwargs.items()):
        # noinspection PyProtectedMember
        types_mapping[name] = arg._op.signature.func.output_type if is_lazy_proxy(arg) else type(arg)

    arg_names = tuple(argspec.args[:len(args)])
    kwarg_names = tuple(kwargs.keys())
    return CallSignature(FuncSignature(f, types_mapping, output_type, arg_names, kwarg_names), args, kwargs)


class LzyExecutionException(Exception):
    def __init__(self, message, *args):
        message += "If you are going to ask for help of cloud support," \
                   " please send the following trace files: /tmp/lzy-log/"
        super().__init__(message, *args)
