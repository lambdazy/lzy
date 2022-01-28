from dataclasses import dataclass
from typing import List

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard
from lzy.servant.terminal_server import TerminalConfig


@dataclass
@whiteboard(namespace="wb", tags=["wb"])
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
config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.id()

config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config) as env:
    wb = env.get_whiteboard(wb_id, SimpleWhiteboard)
    print(len(wb.b))
    wbInfo = env.get_all_whiteboards_info()
    print(wb.a, wb.a, wbInfo[0].status)
