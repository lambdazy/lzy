import uuid

from lzy.api.v2 import op, Lzy
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime


@op
def raises() -> int:
    raise RuntimeError("Bad exception")

if __name__ == "__main__":
    runtime = GrpcRuntime()
    lzy = Lzy(runtime=runtime)
    with lzy.workflow("wf"):
        res = raises()
        print(res)
