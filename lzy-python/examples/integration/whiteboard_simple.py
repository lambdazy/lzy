from dataclasses import dataclass
from lzy.api import op, LzyEnv


@dataclass
class SimpleWhiteboard:
    a: int = 0
    b: str = ""


@op
def fun1() -> int:
    return 42


@op
def fun2(a: int) -> str:
    return str(a)


wb = SimpleWhiteboard()
with LzyEnv(user="test_user", server_url="localhost:8899", whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.id()

with LzyEnv(user="test_user", server_url="localhost:8899") as env:
    wb = env.get_whiteboard(wb_id, SimpleWhiteboard)
    print(wb.a, wb.b)
