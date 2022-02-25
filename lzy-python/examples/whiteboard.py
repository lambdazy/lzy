from dataclasses import dataclass
from typing import List

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard
import uuid


@dataclass
@whiteboard(namespace='wb', tags=["simple_whiteboard"])
class SimpleWhiteboard:
    a: int = 0
    b: List[str] = None


@op
def fun1() -> int:
    return 42


@op
def fun2(a: int) -> List[str]:
    return [str(a), str(a), str(a)]


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

wb = SimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.__id__

env = LzyRemoteEnv()
wb = env.whiteboard(wb_id, SimpleWhiteboard)
print(len(wb.b))
