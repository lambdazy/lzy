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
from typing_extensions import get_origin, get_args
from lzy.proxy.result import Just, Nothing, Result

T = TypeVar("T")

TypeInferResult = Result[Sequence[type]]


def infer_real_type(typ: Type) -> Type:
    origin: Optional[Type] = get_origin(typ)
    if origin is not None:
        if origin == Union:  # type: ignore
            args = get_args(typ)  # TODO: what should we do with real Union?
            if len(args) == 2 and issubclass(args[1], type(None)):  # check type is Optional
                return infer_real_type(args[0])
        return origin
    return typ


def infer_return_type(func: Callable) -> TypeInferResult:
    hints = get_type_hints(func)
    if "return" not in hints:
        return Nothing()

    typ = hints["return"]
    if isinstance(typ, tuple):
        return Just(typ)

    return Just(tuple((typ,)))


def unwrap(val: Optional[T]) -> T:
    assert val is not None, "val is None :c"
    return cast(T, val)
