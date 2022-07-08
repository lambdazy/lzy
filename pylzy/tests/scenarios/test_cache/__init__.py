import uuid

from lzy.api.v1 import CachePolicy, LzyRemoteEnv, op
from lzy.api.v1.lazy_op import LzyOp

WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())


@op
def fun1() -> int:
    return 42


@op
def fun2(a: int, b: int) -> int:
    return a + b


with LzyRemoteEnv().workflow(
    name=WORKFLOW_NAME, cache_policy=CachePolicy.SAVE_AND_RESTORE
):
    a = fun2(fun1(), 1)
    b = fun2(fun1(), 2)

with LzyRemoteEnv().workflow(
    name=WORKFLOW_NAME, cache_policy=CachePolicy.SAVE_AND_RESTORE
):
    c = fun2(fun1(), 1)

op1: LzyOp = a._op
op2: LzyOp = b._op
op3: LzyOp = c._op

print(f"Is fun2 cached? {op1.return_entry_id() == op3.return_entry_id()}")
