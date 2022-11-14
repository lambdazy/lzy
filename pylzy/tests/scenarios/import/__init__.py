import uuid

from base_module.base import Base
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
from some_imported_file_2 import foo

from lzy.api.v2 import op, Lzy


@op
def just_call_imported_stuff() -> str:
    base = Base(1, "before")
    v1, v2 = foo(), base.echo()
    print(v1, v2, sep="\n")
    print("Just print some text")
    return v1 + " " + v2


runtime = GrpcRuntime()
lzy = Lzy(runtime=runtime)

with lzy.workflow(name="wf", interactive=False):
    res = just_call_imported_stuff()
    print(res)
