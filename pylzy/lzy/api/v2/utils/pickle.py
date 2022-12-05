import base64
from typing import Any, Type, TypeVar, cast

import cloudpickle

T = TypeVar("T")


def pickle(obj: T) -> str:
    return base64.b64encode(cloudpickle.dumps(obj)).decode("ascii")


def unpickle(base64_str: str, obj_type: Type[T] = None) -> T:
    t = cloudpickle.loads(base64.b64decode(base64_str.encode("ascii")))
    return cast(T, t)
