import base64
import os
import sys
import json
from typing import Any
from collections import OrderedDict
import time
from pathlib import Path

import cloudpickle

from lzy.api.lazy_op import LzyRemoteOp
from lzy.api.utils import lazy_proxy
from lzy.api.whiteboard.credentials import AmazonCredentials, AzureCredentials, AzureSasCredentials
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.api.storage.storage_client import AmazonClient, AzureClient


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

    if 'LOCAL_MODULES' in os.environ:
        if 'AMAZON' in os.environ:
            data = json.loads(os.environ['AMAZON'])
            client = AmazonClient(AmazonCredentials(data['endpoint'], data['accessToken'], data['secretToken']))
        elif 'AZURE' in os.environ:
            data = json.loads(os.environ['AZURE'])
            client = AzureClient.from_connection_string(AzureCredentials(data['connectionString']))
        elif 'AZURE_SAS' in os.environ:
            data = json.loads(os.environ['AZURE_SAS'])
            client = AzureClient.from_sas(AzureSasCredentials(data['endpoint'], data['signature']))
        else:
            raise ValueError('No storage credentials are provided')

        local_modules: OrderedDict = json.loads(os.environ['LOCAL_MODULES'])
        for name, url in local_modules.items():
            local_module = client.read(url)
            sys.modules[name] = local_module

    print("Loading function")
    func_s: FuncSignature = cloudpickle.loads(base64.b64decode(argv[0].encode("ascii")))
    print("Function loaded: " + func_s.name)
    # noinspection PyShadowingNames
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
