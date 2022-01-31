import dataclasses
from unittest import TestCase

from lzy.api import LzyLocalEnv
from lzy.api.whiteboard import whiteboard, view


@dataclasses.dataclass
@whiteboard(namespace='wb', tags=["simple_whiteboard"])
class WB:
    a: int
    b: int


class WhiteboardTests(TestCase):
    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            pass
        with LzyLocalEnv(whiteboard=wb):
            pass
