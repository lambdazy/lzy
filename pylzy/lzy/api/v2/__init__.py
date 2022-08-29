import functools
import inspect
import sys
import uuid
from typing import Any, Callable, Optional, TypeVar, Sequence

from lzy._proxy.result import Nothing
from lzy.api.v2.call import LzyCall
from lzy.api.v2.lzy import Lzy
from lzy.api.v2.provisioning import Gpu, Provisioning
from lzy.api.v2.proxy_adapter import lzy_proxy
from lzy.api.v2.utils.types import infer_call_signature, infer_return_type
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env import EnvSpec

T = TypeVar("T")  # pylint: disable=invalid-name


import logging


# TODO[ottergottaott]:
def handlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    yield handler


def init_logger():
    logging.basicConfig(
        handlers=handlers(),
        level=logging.INFO,
    )


init_logger()

FuncT = TypeVar(
    "FuncT",
    bound=Callable[..., Any],
)


# pylint: disable=[invalid-name]
def op(
    func: Optional[FuncT] = None,
    *,
    gpu: Gpu = None,
    output_type=None,
):
    def deco(f):
        """
        Decorator which will try to infer return type of function
        and create lazy constructor instead of decorated function.
        """
        nonlocal output_type
        if output_type is None:
            infer_result = infer_return_type(f)
            if isinstance(infer_result, Nothing):
                raise TypeError(
                    f"{f} return type is not annotated. "
                    f"Please for proper use of {op.__name__} "
                    f"annotate return type of your function."
                )
            else:
                output_types = infer_result.value  # expecting multiple return types

        # yep, create lazy constructor and return it
        # instead of function
        return create_lazy_constructor(
            f,
            output_type,
            provisioning,
        )

    provisioning = Provisioning(gpu)

    if func is None:
        return deco

    return deco(func)


def create_lazy_constructor(
    f: Callable[..., Any],
    output_types: Sequence[type],
    provisioning: Provisioning,
) -> Callable[..., Any]:
    @functools.wraps(f)
    def lazy(*args, **kwargs):
        # TODO: defaults?
        active_wflow: LzyWorkflow = LzyWorkflow.get_active()

        signature = infer_call_signature(f, output_types, *args, **kwargs)

        # we need specify globals() for caller site to find all
        # required modules
        caller_globals = inspect.stack()[1].frame.f_globals

        # form env to recreate remotely
        env: EnvSpec = active_wflow._env_provider.provide(caller_globals)

        # create
        lzy_call = LzyCall(
            active_wflow,
            signature,
            provisioning,
            env
        )
        # and register LzyCall
        active_wflow.register_call(lzy_call)

        # Special case for NoneType, just leave op registered and return
        # the real None. LzyEnv later will materialize it anyway.
        #
        # Otherwise `is` checks won't work, for example:
        # >>> @op
        # ... def op_none_operation() -> None:
        # ...      pass
        #
        # >>> obj = op_none_operation()
        # >>> obj is None
        # >>> False
        if len(output_types) == 1:
            if issubclass(output_types[0], type(None)):
                return None
            return lzy_proxy(lzy_call.entry_ids[0], lzy_call.signature.func.output_types[0], lzy_call.parent_wflow)

        return tuple(
            lzy_proxy(lzy_call.entry_ids[i], lzy_call.signature.func.output_types[i], lzy_call.parent_wflow)
            for i in range(len(lzy_call.entry_ids))
        )

    return lazy


# register cloud injections
# noinspection PyBroadException
try:
    from lzy.injections import catboost_injection
except:
    pass
