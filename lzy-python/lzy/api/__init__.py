import functools
import inspect
import logging
from typing import Callable

import sys

from lzy.model.env import PyEnv
from lzy.model.zygote import Gpu, Provisioning
from ._proxy import proxy
from .buses import *
from .env import LzyEnv
from .lazy_op import LzyOp, LzyLocalOp, LzyRemoteOp
from .utils import print_lzy_ops, infer_return_type, is_lazy_proxy, lazy_proxy
from .whiteboard.api import UUIDEntryIdGenerator

logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.root.addHandler(handler)


def op(func: Callable = None, *, gpu: Gpu = None, output_type=None):
    provisioning = Provisioning(gpu)
    if func is None:
        return op_(provisioning, output_type=output_type)
    return op_(provisioning, output_type=output_type)(func)


def op_(provisioning: Provisioning, *, input_types=None, output_type=None):
    def deco(f):
        nonlocal output_type
        if output_type is None:
            output_type = infer_return_type(f)
            if output_type is None:
                raise TypeError(f"{f} return type is not annotated. "
                                f"Please for proper use of {op.__name__} annotate "
                                f"return type of your function.")

        @functools.wraps(f)
        def lazy(*args):
            # TODO: all possible arguments, including **kwargs and defaults
            nonlocal input_types
            nonlocal output_type

            # if input types are not specified then try to get types of
            # operation from args return types
            if input_types is None:
                # noinspection PyProtectedMember
                input_types = tuple(
                    arg._op.return_type if is_lazy_proxy(arg) else type(arg)
                    for arg in args
                )

            current_env = LzyEnv.get_active()
            if current_env is None:
                return f(*args)

            if current_env.is_local():
                lzy_op = LzyLocalOp(f, input_types, output_type, args)
            else:
                # we need specifib globals() for caller site to find all
                # required modules
                caller_globals = inspect.stack()[1].frame.f_globals
                env_name, yaml = current_env.generate_conda_env(caller_globals)

                servant = current_env.servant()
                if not servant:
                    raise RuntimeError("Cannot find servant")
                lzy_op = LzyRemoteOp(servant, f, input_types, output_type,
                                     provisioning, PyEnv(env_name, yaml),
                                     deployed=False, args=args,
                                     entry_id_generator=UUIDEntryIdGenerator(current_env.snapshot_id()))
            current_env.register_op(lzy_op)
            return lazy_proxy(lambda: lzy_op.materialize(), output_type,
                              {'_op': lzy_op})

        return lazy

    return deco
