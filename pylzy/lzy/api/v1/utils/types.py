import uuid
from inspect import getfullargspec
from itertools import chain
from typing import (
    Callable,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
    get_type_hints,
)

from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.snapshot import Snapshot
from lzy.api.v1.utils.proxy_adapter import get_proxy_entry_id, is_lzy_proxy
from lzy.proxy.result import Just, Nothing, Result

T = TypeVar("T")

TypeInferResult = Result[Sequence[type]]


def infer_call_signature(
    f: Callable, output_type: Sequence[type], snapshot: Snapshot, *args, **kwargs
) -> CallSignature:
    types_mapping = {}
    argspec = getfullargspec(f)

    # pylint: disable=protected-access
    for name, arg in chain(zip(argspec.args, args), kwargs.items()):
        # noinspection PyProtectedMember
        if is_lzy_proxy(arg):
            eid = get_proxy_entry_id(arg)
            entry = snapshot.get(eid)
            types_mapping[name] = entry.typ
        else:
            types_mapping[name] = type(arg)

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

    if isinstance(or_type, tuple):
        return Just(tuple(infer_real_type(typ) for typ in or_type))

    or_type = infer_real_type(or_type)
    if isinstance(or_type, type):
        return Just(tuple((or_type,)))

    return Nothing()


def unwrap(val: Optional[T]) -> T:
    assert val is not None, "val is None :c"
    return cast(T, val)