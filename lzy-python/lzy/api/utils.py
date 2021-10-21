from typing import Iterable, Callable, Tuple, get_type_hints, Optional, Any, Type, TypeVar, Dict

# noinspection PyProtectedMember
from lzy.api._proxy import proxy
from .lazy_op import LzyOp


def print_lzy_op(op: LzyOp) -> None:
    input_types = ", ".join(str(t) for t in op.input_types)
    print(f'{op.func} {op.func.__name__}({input_types}) -> {op.return_type}')


# TODO: remove?
def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
    for lzy_op in ops:
        print_lzy_op(lzy_op)


def infer_return_type(func: Callable) -> Optional[type]:
    hints = get_type_hints(func)
    if 'return' not in hints:
        return None

    or_type = hints['return']
    if hasattr(or_type, '__origin__'):
        return or_type.__origin__
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


T = TypeVar('T')


def lazy_proxy(materialization: Callable, return_type: Type[T], obj_attrs: Dict[str, Any]):
    return proxy(
        lambda: materialization(),
        return_type,
        cls_attrs={
            '__lzy_proxied__': True
        },
        obj_attrs=obj_attrs
    )
