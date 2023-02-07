import inspect
from typing import (
    Callable,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    get_type_hints, List, Dict, Any, Tuple,
)

from beartype.door import is_subhint
from serialzy.api import SerializerRegistry
from serialzy.types import EmptyContent
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


def check_types_serialization_compatible(annotation: Type, typ: Type, registry: SerializerRegistry) -> bool:
    if annotation == typ:
        return True

    annotation_types = infer_real_types(annotation)
    for annotation_type in annotation_types:
        serializer = registry.find_serializer_by_type(annotation_type)
        serializable = serializer.supported_types() == typ if \
            isinstance(serializer.supported_types(), Type) else serializer.supported_types()(typ)  # type: ignore
        if serializable:
            return True
    return False


def is_subtype(subtype: Type, supertype: Type) -> bool:
    if subtype == List[EmptyContent]:
        subtype = supertype if is_subhint(supertype, List) else List  # type: ignore
    elif subtype == Tuple[EmptyContent]:
        subtype = supertype if is_subhint(supertype, Tuple) else Tuple  # type: ignore
    elif subtype == Dict[EmptyContent, EmptyContent]:
        subtype = supertype if is_subhint(supertype, Dict) else Dict  # type: ignore
    return is_subhint(subtype, supertype)
