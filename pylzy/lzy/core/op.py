from __future__ import annotations

from typing import Any, Callable, Optional, Sequence, TypeVar

from lzy.api.v1.utils.types import infer_return_type
from lzy.core.call import LazyCallWrapper
from lzy.env.environment import LzyEnvironment
from lzy.proxy.result import Absence
from lzy.utils.format import pretty_function


FuncT = TypeVar(
    "FuncT",
    bound=Callable[..., Any],
)


def op(
    func: Optional[FuncT] = None,
    *,
    env: Optional[LzyEnvironment] = None,
    output_types: Optional[Sequence[type]] = None,
    description: str = "",
    version: str = "0.0",
    cache: bool = False,
    lazy_arguments: bool = False,
):
    def deco(f):
        """
        Decorator which will try to infer return type of function
        and create lazy constructor instead of decorated function.
        """

        nonlocal output_types
        if output_types is None:
            infer_result = infer_return_type(f)
            if isinstance(infer_result, Absence):
                raise TypeError(
                    f"Return type is not annotated for {pretty_function(f)}. "
                    f"Please for proper use of {op.__name__} "
                    f"annotate return type of your function."
                )

            output_types = infer_result.value  # expecting multiple return types

        # yep, create lazy constructor and return it
        # instead of function
        return LazyCallWrapper(
            function=f,
            output_types=output_types,
            env=env or LzyEnvironment(),
            description=description,
            version=version,
            cache=cache,
            lazy_arguments=lazy_arguments
        )

    if func is None:
        return deco

    return deco(func)
