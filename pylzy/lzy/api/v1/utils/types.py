import inspect
from typing import (
    Callable,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    get_type_hints, List, Dict, Any,
)

from typing_extensions import get_origin, get_args

from lzy.proxy.result import Just, Nothing, Result

T = TypeVar("T")

TypeInferResult = Result[Sequence[type]]


def infer_real_types(typ: Type) -> Sequence[Type]:
    origin: Optional[Type] = get_origin(typ)
    result: List[Type] = []
    if origin is not None:
        if origin == Union:  # type: ignore
            args = get_args(typ)
            for arg in args:
                result.extend(infer_real_types(arg))
            return result
        return origin,
    return typ,


def infer_return_type(func: Callable) -> TypeInferResult:
    hints = get_type_hints(func)
    if "return" not in hints:
        return Nothing()

    typ = hints["return"]
    if isinstance(typ, tuple):
        return Just(typ)

    return Just(tuple((typ,)))


def get_default_args(func: Callable) -> Dict[str, Any]:
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
