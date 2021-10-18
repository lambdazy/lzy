import base64
import inspect
import os
from pathlib import Path
from typing import Any

import cloudpickle
import sys

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import infer_return_type
from lzy.api.utils import lazy_proxy
from servant.bash_servant import BashServant


def load_arg(path: Path) -> Any:
    with open(path, 'rb') as f:
        return cloudpickle.load(f)


def main():
    argv = sys.argv[1:]
    servant = BashServant()

    print("Loading function")
    func = cloudpickle.loads(base64.b64decode(argv[0].encode('ascii')))
    print("Function loaded: " + func.__name__)

    params = inspect.signature(func).parameters
    args = tuple(
        lazy_proxy(lambda name=name: load_arg(os.path.join(os.path.sep, servant.mount(), func.__name__, name)),
                   value.annotation,
                   {})
        for name, value in params.items()
    )
    print(f"Loaded {len(args)} args")

    print(f'Running {func.__name__}')
    op = LzyRemoteOp(servant, func, tuple(v.annotation for k, v in params.items()), infer_return_type(func), *args)
    op.deploy()
    result = op.materialize()
    print(f'Result of execution {result}')

    result_path = os.path.join(os.path.sep, servant.mount(), func.__name__, "return")
    print(f'Writing result to file {result_path}')
    with open(result_path, 'wb') as out_handle:
        cloudpickle.dump(result, out_handle)


if __name__ == "__main__":
    main()
