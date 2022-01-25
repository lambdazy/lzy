from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Union,
    Type,
    Tuple,
    TypeVar,
    get_type_hints,
)

from lzy.api._proxy import proxy
from lzy.api.lazy_op import LzyOp

T = TypeVar("T")  # pylint: disable=invalid-name


# TODO: remove?
def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
    for lzy_op in ops:
        # noinspection PyProtectedMember
        # pylint: disable=protected-access
        print(repr(lzy_op.signature.func))


class NoResult:
    pass


@dataclass
class Result:
    inferred_type: type


TypeInferResult = Union[Result, NoResult]


def infer_return_type(func: Callable) -> TypeInferResult:
    hints = get_type_hints(func)
    if "return" not in hints:
        return NoResult()

    or_type = hints["return"]
    if or_type == type(None):
        return Result(None)

    if hasattr(or_type, "__origin__"):
        return Result(or_type.__origin__)  # type: ignore

    if isinstance(or_type, type):
        return Result(or_type)

    return NoResult()


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
