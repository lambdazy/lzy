import functools
import inspect
import logging
from typing import Callable

import sys

from lzy.api.env import LzyEnv
from lzy.api.lazy_op import LzyLocalOp, LzyRemoteOp
from lzy.api.utils import infer_return_type, is_lazy_proxy, lazy_proxy, NoResult
from lzy.api.whiteboard.api import UUIDEntryIdGenerator

from lzy.model.signatures import FuncSignature, CallSignature
from lzy.model.zygote import Provisioning, Gpu
from lzy.model.env import PyEnv


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
            if isinstance(infer_result, NoResult):
                raise TypeError(
                    f"{f} return type is not annotated. "
                    f"Please for proper use of {op.__name__} "
                    f"annotate return type of your function."
                )
            else:
                output_type = infer_result.inferred_type

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

            current_env = LzyEnv.get_active()
            if current_env is None:
                return f(*args)

            signature = CallSignature(FuncSignature(f, input_types, output_type), args)

            if current_env.is_local():
                lzy_op = LzyLocalOp(signature)
            else:
                # we need specify globals() for caller site to find all
                # required modules
                caller_globals = inspect.stack()[1].frame.f_globals
                env_name, yaml = current_env.generate_conda_env(caller_globals)

                servant = current_env.servant()
                if not servant:
                    raise RuntimeError("Cannot find servant")
                id_generator = UUIDEntryIdGenerator(current_env.snapshot_id())
                pyenv = PyEnv(env_name, yaml)
                lzy_op = LzyRemoteOp(
                    servant,
                    signature,
                    provisioning,
                    pyenv,
                    deployed=False,
                    entry_id_generator=id_generator,
                )
            current_env.register_op(lzy_op)
            return lazy_proxy(lzy_op.materialize, output_type, {"_op": lzy_op})

        return lazy

    return deco
