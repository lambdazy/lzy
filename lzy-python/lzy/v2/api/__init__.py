import functools
import inspect
import logging
import sys
import uuid
from typing import Callable

from lzy.v2._proxy.result import Nothing
from lzy.v2.api.lzy_call import LzyCall
from lzy.v2.api.lzy_op import LzyOp
from lzy.v2.api.lzy_workflow import LzyWorkflow
from lzy.v2.servant.model.provisioning import Provisioning, Gpu
from lzy.v2.utils import infer_return_type, infer_call_signature, lazy_proxy

logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.root.addHandler(handler)


# TODO: another provisioning?
# pylint: disable=[invalid-name]
def op(func: Callable = None, *, gpu: Gpu = None, output_type=None):
    provisioning = Provisioning(gpu)
    if func is None:
        return op_(provisioning, output_type=output_type)
    return op_(provisioning, output_type=output_type)(func)


# pylint: disable=unused-argument
def op_(provisioning: Provisioning, *, output_type=None):
    def deco(f):
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

        @functools.wraps(f)
        def lazy(*args, **kwargs):
            # TODO: defaults?
            current_workflow = LzyWorkflow.get_active()
            if current_workflow is None:
                return f(*args, **kwargs)

            nonlocal output_type
            signature = infer_call_signature(f, output_type, *args, **kwargs)
            env_provider = current_workflow.owner().env_provider

            # we need specify globals() for caller site to find all
            # required modules
            caller_globals = inspect.stack()[1].frame.f_globals

            lzy_op = LzyOp(
                env_provider.for_op(caller_globals),
                provisioning,
                signature.func.callable,
                signature.func.input_types,
                signature.func.output_type,
                signature.func.arg_names,
                signature.func.kwarg_names
            )

            lzy_call = LzyCall(
                lzy_op,
                args,
                kwargs,
                str(uuid.uuid4())
            )

            current_workflow.call(lzy_call)

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
            else:
                return lazy_proxy(lzy_call.execute, output_type, {"lzy_call": lzy_call})

        return lazy


    return deco

# register cloud injections
# noinspection PyBroadException
try:
    from lzy.v2.injections import catboost_injection
except:
    pass