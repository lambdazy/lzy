import base64
import inspect
import os
from pathlib import Path
from typing import Any

import cloudpickle
import sys

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import lazy_proxy
from lzy.model.zygote_python_func import FuncContainer
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient


def load_arg(path: Path) -> Any:
    with open(path, 'rb') as f:
        return cloudpickle.load(f)


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient()

    print("Loading function")
    container: FuncContainer = cloudpickle.loads(base64.b64decode(argv[0].encode('ascii')))
    print("Function loaded: " + container.func.__name__)

    params_names = list(inspect.signature(container.func).parameters)
    args = tuple(
        lazy_proxy(
            lambda i=i: load_arg(os.path.join(os.path.sep, servant.mount(), container.func.__name__, params_names[i])),
            container.input_types[i],
            {})
        for i in range(len(params_names))
    )
    print(f"Loaded {len(args)} args")

    print(f'Running {container.func.__name__}')
    op = LzyRemoteOp(servant, container.func, container.input_types,
                     container.output_type, deployed=True, args=args)
    result = op.materialize()
    print(f'Result of execution {result}')

    result_path = os.path.join(os.path.sep, servant.mount(), container.func.__name__, "return")
    print(f'Writing result to file {result_path}')
    with open(result_path, 'wb') as out_handle:
        cloudpickle.dump(result, out_handle)


if __name__ == "__main__":
    main()
