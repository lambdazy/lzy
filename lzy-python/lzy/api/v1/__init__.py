import functools
import inspect
import logging
import sys
from typing import Callable

from lzy.api.v1.env import LzyWorkflowBase, LzyRemoteWorkflow, LzyLocalWorkflow
from lzy.api.v1.lazy_op import LzyLocalOp, LzyRemoteOp
from lzy._proxy.result import Nothing
from lzy.api.v1.servant.model.zygote import Gpu, Provisioning
from lzy.api.v1.whiteboard.model import UUIDEntryIdGenerator
from lzy.api.v1.utils import infer_return_type, lazy_proxy, infer_call_signature

logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.root.addHandler(handler)


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
            current_workflow = LzyWorkflowBase.get_active()
            if current_workflow is None:
                return f(*args, **kwargs)

            nonlocal output_type
            signature = infer_call_signature(f, output_type, *args, **kwargs)
            if isinstance(current_workflow, LzyLocalWorkflow):
                lzy_op = LzyLocalOp(signature)
            elif isinstance(current_workflow, LzyRemoteWorkflow):
                servant = current_workflow.servant()
                if not servant:
                    raise RuntimeError("Cannot find servant")
                id_generator = UUIDEntryIdGenerator(current_workflow.snapshot_id())

                # we need specify globals() for caller site to find all
                # required modules
                caller_globals = inspect.stack()[1].frame.f_globals
                pyenv = current_workflow.py_env(caller_globals)

                lzy_op = LzyRemoteOp(
                    servant,
                    signature,
                    current_workflow.snapshot_id(),
                    id_generator,
                    current_workflow.mem_serializer(),
                    current_workflow.file_serializer(),
                    current_workflow.hasher(),
                    provisioning,
                    pyenv,
                    deployed=False,
                    channel_manager=current_workflow.channel_manager(),
                    cache_policy=current_workflow.cache_policy
                )
            else:
                raise TypeError(f"Unsupported env type: {type(current_workflow)}")
            current_workflow.register_op(lzy_op)

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
                return lazy_proxy(lzy_op.materialize, output_type, {"_op": lzy_op})

        return lazy

    return deco


# register cloud injections
# noinspection PyBroadException
try:
    from lzy.injections import catboost_injection
except:
    pass
