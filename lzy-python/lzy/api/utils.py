from typing import (
    Any,
    Callable,
    Dict,
    Type,
    Tuple,
    TypeVar,
    get_type_hints,
)

# noinspection PyProtectedMember
from lzy.api._proxy import proxy
from lzy.api.result import Result, Just, Nothing

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
