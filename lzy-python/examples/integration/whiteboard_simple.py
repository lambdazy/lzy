from dataclasses import dataclass
from typing import List

from lzy.api import op, LzyEnv


@dataclass
class SimpleWhiteboard:
    a: int = 0
    b: List[str] = None


@op
def fun1() -> int:
    return 42


@op
def fun2(a: int) -> List[str]:
    return [str(a), str(a), str(a)]


wb = SimpleWhiteboard()
with LzyEnv(user="test_user", server_url="localhost:8899", whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.id()

with LzyEnv(user="test_user", server_url="localhost:8899") as env:
    wb = env.get_whiteboard(wb_id, SimpleWhiteboard)
    print(len(wb.b))
    wbInfo = env.get_all_whiteboards_info()
    print(wb.a, wb.a, wbInfo[0].status)
