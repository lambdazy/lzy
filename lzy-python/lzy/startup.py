import base64
import os
import sys
import json
from typing import Any
import time
from pathlib import Path

import cloudpickle

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import lazy_proxy
from lzy.api.whiteboard.credentials import AmazonCredentials, AzureCredentials
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.whiteboard_storage import AmazonClient, AzureClient


def load_arg(path: Path) -> Any:
    with open(path, "rb") as file:
        # Wait for slot become open
        while file.read(1) is None:
            time.sleep(0)  # Thread.yield
        file.seek(0)
        return cloudpickle.load(file)


def main():
    argv = sys.argv[1:]
    servant: ServantClient = BashServantClient.instance()

    if os.environ['LOCAL_MODULES'] is not None:
        credentials, bucket = servant.get_credentials_and_bucket(ServantClient.CredentialsTypes.S3)

        if isinstance(credentials, AmazonCredentials):
            client = AmazonClient(credentials)
        if isinstance(credentials, AzureCredentials):
            client = AzureClient.from_connection_string(credentials)
        else:
            client = AzureClient.from_sas(credentials)

        local_modules = json.loads(os.environ['LOCAL_MODULES'])
        for name, url in local_modules:
            local_module = client.read(url)
            sys.modules[name] = local_module

    print("Loading function")
    func_s: FuncSignature = cloudpickle.loads(base64.b64decode(argv[0].encode("ascii")))
    print("Function loaded: " + func_s.name)
    args = tuple(
        lazy_proxy(
            lambda name=name: load_arg(servant.mount() / func_s.name / name),
            inp_type,
            {},
        )
        for name, inp_type in zip(func_s.param_names, func_s.input_types)
    )
    lazy_call = CallSignature(func_s, args)
    print(f"Loaded {len(args)} lazy args")

    print(f"Running {func_s.name}")
    op_ = LzyRemoteOp(servant, lazy_call, deployed=True)
    result = op_.materialize()
    print(f"Result of execution {result}")

    result_path = servant.mount() / func_s.name / "return"
    print(f"Writing result to file {result_path}")
    with open(result_path, "wb") as out_handle:
        cloudpickle.dump(result, out_handle)
        out_handle.flush()
        os.fsync(out_handle.fileno())


if __name__ == "__main__":
    main()
