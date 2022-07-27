import functools
import inspect
import sys
import uuid
from typing import Any, Callable, Optional, TypeVar

from lzy._proxy.result import Nothing
from lzy.api.v2.api.lzy import Lzy
from lzy.api.v2.api.lzy_call import LzyCall
from lzy.api.v2.api.lzy_workflow import LzyWorkflow
from lzy.api.v2.api.provisioning import Gpu, Provisioning
from lzy.api.v2.proxy_adapter import lzy_proxy
from lzy.api.v2.servant.model.zygote import python_func_zygote
from lzy.api.v2.utils import infer_call_signature, infer_return_type
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
        encoding="utf-8",
        level=logging.INFO,
    )


init_logger()


# pylint: disable=[invalid-name]
def op(
    func: Callable = None,
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
                output_type = infer_result.value

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
    output_type: type,
    provisioning: Provisioning,
) -> Callable[..., Any]:
    @functools.wraps(f)
    def lazy(*args, **kwargs):
        wflow_ = LzyWorkflow.get_active()
        # TODO: defaults?
        if wflow_ is None:
            return f(*args, **kwargs)
        active_wflow: LzyWorkflow = wflow_

        signature = infer_call_signature(f, output_type, *args, **kwargs)

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
            env,
            str(uuid.uuid4()),
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
        if issubclass(output_type, type(None)):
            return None

        return lzy_proxy(lzy_call)

    return lazy


# register cloud injections
# noinspection PyBroadException
try:
    from lzy.injections import catboost_injection
except:
    pass
