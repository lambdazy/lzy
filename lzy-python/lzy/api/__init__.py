import functools
import inspect
import logging
import sys
from typing import Callable

from lzy.api.env import LzyEnvBase, LzyRemoteEnv, LzyLocalEnv
from lzy.api.lazy_op import LzyLocalOp, LzyRemoteOp
from lzy.api.result import Nothing
from lzy.api.utils import infer_return_type, is_lazy_proxy, lazy_proxy
from lzy.api.whiteboard.model import UUIDEntryIdGenerator
from lzy.model.signatures import FuncSignature, CallSignature
from lzy.model.zygote import Provisioning, Gpu

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
        def lazy(*args):
            # TODO: all possible arguments, including **kwargs and defaults
            nonlocal output_type

            # noinspection PyProtectedMember
            # pylint: disable=protected-access
            input_types = tuple(
                arg._op.signature.func.output_type if is_lazy_proxy(arg) else type(arg)
                for arg in args
            )

            current_env = LzyEnvBase.get_active()
            if current_env is None:
                return f(*args)

            signature = CallSignature(FuncSignature(f, input_types, output_type), args)

            if isinstance(current_env, LzyLocalEnv):
                lzy_op = LzyLocalOp(signature)
            elif isinstance(current_env, LzyRemoteEnv):
                servant = current_env.servant()
                if not servant:
                    raise RuntimeError("Cannot find servant")
                id_generator = UUIDEntryIdGenerator(current_env.snapshot_id())

                # we need specify globals() for caller site to find all
                # required modules
                caller_globals = inspect.stack()[1].frame.f_globals
                pyenv = current_env.py_env(caller_globals)

                lzy_op = LzyRemoteOp(
                    servant,
                    signature,
                    provisioning,
                    pyenv,
                    deployed=False,
                    entry_id_generator=id_generator,
                )
            else:
                raise RuntimeError(f"Unsupported env type: {type(current_env)}")
            current_env.register_op(lzy_op)

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
