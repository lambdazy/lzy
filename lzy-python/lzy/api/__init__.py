import functools
import logging
from typing import Callable, get_type_hints, Any

import sys

from ._proxy import proxy
from api.lazy_op import LzyOp, LzyLocalOp, LzyRemoteOp
from .buses import *
from .env import LzyEnv
from .utils import print_lzy_ops

logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.root.addHandler(handler)


def op(func: Callable = None, *, output_type=None):
    if func is None:
        if output_type is None:
            raise ValueError(f'output_type should be not None')
        return op_(output_type=output_type)

    # TODO: infer input args type information
    return_type = _infer_return_type(func)
    return op_(output_type=return_type)(func)


def _infer_return_type(func):
    hints = get_type_hints(func)
    if 'return' not in hints:
        raise TypeError(f"{func} return type is not annotated."
                        f"Please for proper use of {op.__name__} annotate "
                        f"return type of your function.")

    or_type = hints['return']
    if hasattr(or_type, '__origin__'):
        return or_type.__origin__
    elif type(or_type) == type:
        return or_type
    else:
        raise TypeError(f'Cannot infer op ({func}) return type')


def op_(*, input_types=None, output_type=None):
    def deco(f):
        @functools.wraps(f)
        def lazy(*args):
            # TODO: all possible arguments, including **kwargs and defaults
            nonlocal input_types

            # if input types are not specified then try to get types of
            # operation from args return types
            if input_types is None:
                # TODO: should exception be raised if not lazyproxy?
                input_types = tuple(
                    arg._op.return_type if islazyproxy(arg) else type(arg)
                    for arg in args
                )

            current_env = LzyEnv.get_active()
            if current_env is None:
                return f(*args)

            if current_env.is_local():
                lzy_op = LzyLocalOp(f, input_types, output_type, *args)
            else:
                lzy_op = LzyRemoteOp(f, input_types, output_type, *args)
            current_env.register_op(lzy_op)
            return lazy_op_proxy(lzy_op, output_type)

        return lazy

    return deco


def lazy_op_proxy(op: LzyOp, return_type: type):
    return proxy(
        lambda: op.materialize(),
        return_type,
        cls_attrs={
            '__lzy_proxied__': True
        },
        obj_attrs={
            '_op': op
        }
    )


def islazyproxy(obj: Any):
    cls = type(obj)
    return hasattr(cls, '__lzy_proxied__') and cls.__lzy_proxied__
