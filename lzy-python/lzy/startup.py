import base64
import inspect
import os
from typing import Any

import sys
import cloudpickle
from pathlib import Path

from lzy.api.utils import infer_arg_types, infer_return_type
from lzy.api.lazy_op import LzyRemoteOp

from servant.bash_servant import BashServant


def load_arg(path: Path) -> Any:
    with open(path, 'rb') as f:
        return cloudpickle.load(f)


def main():
    args = sys.argv[1:]
    servant = BashServant()

    print("Loading function")
    func = cloudpickle.loads(base64.b64decode(args[0].encode('ascii')))
    print("Function loaded: " + func.__name__)

    # TODO: lazy args loading
    args = tuple(
        load_arg(os.path.join(os.path.sep, servant.mount(), func.__name__, arg_name))
        for arg_name in inspect.getfullargspec(func).args
    )
    print(f"Loaded {len(args)} args")

    print(f'Running {func.__name__}')
    op = LzyRemoteOp(servant, func, infer_arg_types(args), infer_return_type(func), *args)
    op.deploy()
    result = op.materialize()
    print(f'Result of execution {result}')

    result_path = os.path.join(os.path.sep, servant.mount(), func.__name__, "return")
    print(f'Writing result to file {result_path}')
    with open(result_path, 'wb') as out_handle:
        cloudpickle.dump(result, out_handle)


if __name__ == "__main__":
    main()
