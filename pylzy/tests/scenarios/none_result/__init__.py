from lzy.api.v2 import op, Lzy
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime


@op
def just_return_none() -> None:
    return None


runtime = GrpcRuntime()
lzy = Lzy(runtime=runtime)

with lzy.workflow(name="wf", interactive=False):
    res = just_return_none()
    print(res)
