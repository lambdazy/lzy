from typing import Iterable, Callable, Tuple, get_type_hints, Optional, Any, Type, Dict, TypeVar

# noinspection PyProtectedMember
from lzy.api._proxy import proxy
from .lazy_op import LzyOp

T = TypeVar('T')


# TODO: remove?
def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
    for lzy_op in ops:
        print(repr(lzy_op.signature.func))


def infer_return_type(func: Callable) -> Optional[type]:
    hints = get_type_hints(func)
    if 'return' not in hints:
        return None

    or_type = hints['return']
    if hasattr(or_type, '__origin__'):
        return or_type.__origin__  # type: ignore
    elif type(or_type) == type:
        return or_type
    else:
        return None


def infer_arg_types(*args) -> Tuple[type, ...]:
    # noinspection PyProtectedMember
    return tuple(
        arg._op.return_type if is_lazy_proxy(arg) else type(arg)
        for arg in args
    )


def is_lazy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, '__lzy_proxied__') and cls.__lzy_proxied__


def lazy_proxy(materialization: Callable[[], T], return_type: Type[T], obj_attrs: Dict[str, Any]) -> Any:
    return proxy(
        lambda: materialization(),
        return_type,
        cls_attrs={
            '__lzy_proxied__': True
        },
        obj_attrs=obj_attrs
    )
