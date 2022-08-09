import base64
from typing import Any, Type, TypeVar, cast

import cloudpickle

T = TypeVar("T")


def unpickle_type(base64_str: str) -> type:
    t_ = unpickle_type(base64_str)
    if not isinstance(t_, type):
        raise TypeError(f"cannot upickle type from {base64_str}")
    return cast(type, t_)


def pickle(obj: T) -> str:
    return base64.b64encode(cloudpickle.dumps(obj)).decode("ascii")


def unpickle(base64_str: str, obj_type: Type[T] = None) -> T:
    t_ = cloudpickle.loads(base64.b64decode(base64_str))
    return cast(T, t_)
