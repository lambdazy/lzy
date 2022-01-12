import base64
import os
from pathlib import Path
from typing import Any

import cloudpickle
import sys
import time

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import lazy_proxy
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient


def load_arg(path: Path) -> Any:
    with open(path, 'rb') as f:
        # Wait for slot become open
        while f.read(1) is None:
            time.sleep(0)  # Thread.yield
        f.seek(0)
        return cloudpickle.load(f)


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance()

    print("Loading function")
    func_s: FuncSignature = cloudpickle.loads(base64.b64decode(argv[0].encode('ascii')))
    print("Function loaded: " + func_s.name)
    args = tuple(
        lazy_proxy(
            lambda name=name: load_arg(servant.mount() / func_s.name / name),
            inp_type,
            {}
        )
        for name, inp_type in zip(func_s.param_names, func_s.input_types)
    )
    lazy_call = CallSignature(func_s, args)
    print(f"Loaded {len(args)} lazy args")

    print(f'Running {func_s.name}')
    op = LzyRemoteOp(servant, lazy_call, deployed=True)
    result = op.materialize()
    print(f'Result of execution {result}')

    result_path = servant.mount() / func_s.name / "return"
    print(f'Writing result to file {result_path}')
    with open(result_path, 'wb') as out_handle:
        cloudpickle.dump(result, out_handle)
        out_handle.flush()
        os.fsync(out_handle.fileno())


if __name__ == "__main__":
    main()
