from dataclasses import dataclass

from lzy.api.v2 import Lzy
from lzy.api.v2.lzy import whiteboard


@whiteboard(name="aaa")
@dataclass
class Wb:
    a: int
    b: str = "str"


lzy = Lzy()
with lzy.workflow("name") as wf:
    wf.create_whiteboard(Wb)
